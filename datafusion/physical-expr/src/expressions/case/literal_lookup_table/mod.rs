// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

mod boolean_index_map;
mod bytes_like_index_map;
mod primitive_index_map;

use crate::expressions::case::literal_lookup_table::boolean_index_map::BooleanIndexMap;
use crate::expressions::case::literal_lookup_table::bytes_like_index_map::{
    BytesDictionaryHelper, BytesLikeIndexMap, BytesViewDictionaryHelper,
    FixedBinaryHelper, FixedBytesDictionaryHelper, GenericBytesHelper,
    GenericBytesViewHelper,
};
use crate::expressions::case::literal_lookup_table::primitive_index_map::PrimitiveIndexMap;
use crate::expressions::case::CaseBody;
use crate::expressions::Literal;
use arrow::array::{downcast_integer, downcast_primitive, ArrayRef, UInt32Array};
use arrow::datatypes::{
    ArrowDictionaryKeyType, BinaryViewType, DataType, GenericBinaryType,
    GenericStringType, StringViewType,
};
use datafusion_common::DataFusionError;
use datafusion_common::{arrow_datafusion_err, plan_datafusion_err, ScalarValue};
use indexmap::IndexMap;
use std::fmt::Debug;

/// An optimized scalar value lookup table.
#[derive(Debug)]
pub(in super::super) struct LiteralLookupTable {
    /// The index map that maps keys to indices in the `values` array
    key_to_value_index_map: Box<dyn ScalarIndexMap>,

    /// The index of the default value in the `values` array
    default_value_index: u32,

    /// The value array of this map. The indices in `key_to_value_index_map` and `default_value_index`
    /// are indices in this array
    values: ArrayRef,
}

impl LiteralLookupTable {
    pub(in super::super) fn map_keys_to_values(
        &self,
        keys_array: &ArrayRef,
    ) -> datafusion_common::Result<ArrayRef> {
        let value_indices = self
            .key_to_value_index_map
            .map_to_indices(keys_array, self.default_value_index)?;

        // Zero-copy conversion
        let value_indices = UInt32Array::from(value_indices);

        // An optimized version would depend on the type of `values`.
        // For example, if the type is 'view', we can just keep pointing to the same value (similar to dictionary)
        // if the type is 'dictionary', we can just use the indices as is (or cast them to the key type) and create a new dictionary array
        let values = arrow::compute::take(&self.values, &value_indices, None)
            .map_err(|e| arrow_datafusion_err!(e))?;

        Ok(values)
    }
}

/// Map scalar values to integer indices
trait ScalarIndexMap: Debug + Send + Sync {
    /// Given an array of values, returns a vector of corresponding indices.
    /// The returned indices are the indices of the values in the `Vec` that was used to
    /// create the index map.
    ///
    /// When a literal index map was created with, for example, the `Vec`
    ///
    /// ```text
    /// vec![
    ///     <scalar_a>,
    ///     <scalar_b>,
    ///     <scalar_c>,
    ///     <scalar_d>,
    /// ]
    /// ```
    ///
    /// a call to this method with an `array` that consists of:
    /// - `[<scalar_a>, <scalar_c>, <scalar_x>, <scalar_b>, <scalar_a>]`
    ///
    /// the returned vector will be:
    /// - `[0, 2, default_value, 1, 0]`
    fn map_to_indices(
        &self,
        array: &ArrayRef,
        default_value: u32,
    ) -> datafusion_common::Result<Vec<u32>>;
}

impl TryFrom<&CaseBody> for LiteralLookupTable {
    type Error = ();

    /// Tries to derive a `LiteralLookupTable` from a `CASE` expression.
    ///
    /// When the given `CASE` has the form:
    ///
    /// ```sql
    /// CASE <expr>
    ///     WHEN <literal_key_a> THEN <literal_value_a>
    ///     WHEN <literal_key_b> THEN <literal_value_b>
    ///     ...
    ///     WHEN <literal_key_n> THEN <literal_value_n>
    ///     [ELSE <literal_else_value>]
    /// END
    /// ```
    ///
    /// with more than one `WHEN` branch, this function will return a `Some` value.
    ///
    /// The returned `LiteralLookupTable` will be able to map `expr` values from `WHEN` keys to the
    /// corresponding `THEN` value or the `ELSE` value (or `NULL`) if an `expr` value does not
    /// match any literal `WHEN` key.
    ///
    /// Otherwise, `None` is returned.
    ///
    /// # Improvement idea
    /// TODO - we should think of unwrapping the `IN` expressions into multiple equality comparisons
    /// so it will use this optimization as well, e.g.
    /// ```sql
    /// -- Before
    /// CASE
    ///     WHEN (<expr_a> = <literal_a>) THEN <literal_e>
    ///     WHEN (<expr_a> in (<literal_b>, <literal_c>) THEN <literal_f>
    ///     WHEN (<expr_a> = <literal_d>) THEN <literal_g>
    /// ELSE <optional-fallback_literal>
    ///
    /// -- After
    /// CASE
    ///     WHEN (<expr_a> = <literal_a>) THEN <literal_e>
    ///     WHEN (<expr_a> = <literal_b>) THEN <literal_f>
    ///     WHEN (<expr_a> = <literal_c>) THEN <literal_g>
    ///     WHEN (<expr_a> = <literal_d>) THEN <literal_h>
    ///     ELSE <optional-fallback_literal>
    /// END
    /// ```
    fn try_from(body: &CaseBody) -> Result<Self, Self::Error> {
        // We can't use the optimization if we don't have any when then pairs
        if body.when_then_expr.is_empty() {
            return Err(());
        }

        // If we only have 1 than this optimization is not useful
        if body.when_then_expr.len() == 1 {
            return Err(());
        }

        // Try to downcast all the WHEN/THEN expressions to literals
        let when_then_exprs_maybe_literals = body
            .when_then_expr
            .iter()
            .map(|(when, then)| {
                let when_maybe_literal = when.as_any().downcast_ref::<Literal>();
                let then_maybe_literal = then.as_any().downcast_ref::<Literal>();

                when_maybe_literal.zip(then_maybe_literal)
            })
            .collect::<Vec<_>>();

        // If not all the WHEN/THEN expressions are literals we cannot use this optimization
        if when_then_exprs_maybe_literals.contains(&None) {
            return Err(());
        }

        let when_then_exprs_scalars = when_then_exprs_maybe_literals
            .into_iter()
            // Unwrap the options as we have already checked there is no None
            .flatten()
            .map(|(when_lit, then_lit)| {
                (when_lit.value().clone(), then_lit.value().clone())
            })
            // Only keep non-null WHEN literals
            // as they cannot be matched - case NULL WHEN NULL THEN ... ELSE ... END always goes to ELSE
            .filter(|(when_lit, _)| !when_lit.is_null())
            .collect::<Vec<_>>();

        if when_then_exprs_scalars.is_empty() {
            // All WHEN literals were nulls, so cannot use optimization
            //
            // instead, another optimization would be to go straight to the ELSE clause
            return Err(());
        }

        // Keep only the first occurrence of each when scalar (as the first match is used)
        // and remove nulls (as they cannot be matched - case NULL WHEN NULL THEN ... ELSE ... END always goes to ELSE)
        let (when, then): (Vec<ScalarValue>, Vec<ScalarValue>) = {
            let mut map = IndexMap::with_capacity(body.when_then_expr.len());

            for (when, then) in when_then_exprs_scalars.into_iter() {
                // Don't overwrite existing entries as we want to keep the first occurrence
                if !map.contains_key(&when) {
                    map.insert(when, then);
                }
            }

            map.into_iter().unzip()
        };

        let else_value: ScalarValue = if let Some(else_expr) = &body.else_expr {
            let literal = else_expr.as_any().downcast_ref::<Literal>().ok_or(())?;

            literal.value().clone()
        } else {
            let Ok(null_scalar) = ScalarValue::try_new_null(&then[0].data_type()) else {
                return Err(());
            };

            null_scalar
        };

        {
            let data_type = when[0].data_type();

            // If not all the WHEN literals have the same data type we cannot use this optimization
            if when.iter().any(|l| l.data_type() != data_type) {
                return Err(());
            }
        }

        {
            let data_type = then[0].data_type();

            // If not all the then and the else literals have the same data type we cannot use this optimization
            if then.iter().any(|l| l.data_type() != data_type) {
                return Err(());
            }

            if else_value.data_type() != data_type {
                return Err(());
            }
        }

        let then_and_else_values = ScalarValue::iter_to_array(
            then.iter()
                // The else is in the end
                .chain(std::iter::once(&else_value))
                .cloned(),
        )
        .map_err(|_| ())?;

        // The else expression is the last element in the array
        let else_index = then_and_else_values.len() as u32 - 1;

        let when_to_value_index_map = try_create_index_map(when).map_err(|_| ())?;

        Ok(Self {
            key_to_value_index_map: when_to_value_index_map,
            values: then_and_else_values,
            default_value_index: else_index,
        })
    }
}

fn try_create_index_map(
    unique_non_null_scalars: Vec<ScalarValue>,
) -> datafusion_common::Result<Box<dyn ScalarIndexMap>> {
    assert_ne!(
        unique_non_null_scalars.len(),
        0,
        "Must have at least one scalar value"
    );
    match unique_non_null_scalars[0].data_type() {
        DataType::Boolean => {
            let index_map = BooleanIndexMap::try_from(unique_non_null_scalars)?;
            Ok(Box::new(index_map))
        }

        data_type if data_type.is_primitive() => {
            macro_rules! create_matching_map {
                ($t:ty) => {{
                    let lookup_table =
                        PrimitiveIndexMap::<$t>::try_from(unique_non_null_scalars)?;
                    Ok(Box::new(lookup_table))
                }};
            }

            downcast_primitive! {
                data_type => (create_matching_map),
                _ => Err(plan_datafusion_err!(
                    "Unsupported field type for primitive: {:?}",
                    data_type
                )),
            }
        }

        DataType::Utf8 => {
            let index_map = BytesLikeIndexMap::<
                GenericBytesHelper<GenericStringType<i32>>,
            >::try_from(unique_non_null_scalars)?;
            Ok(Box::new(index_map))
        }

        DataType::LargeUtf8 => {
            let index_map = BytesLikeIndexMap::<
                GenericBytesHelper<GenericStringType<i64>>,
            >::try_from(unique_non_null_scalars)?;
            Ok(Box::new(index_map))
        }

        DataType::Binary => {
            let index_map = BytesLikeIndexMap::<
                GenericBytesHelper<GenericBinaryType<i32>>,
            >::try_from(unique_non_null_scalars)?;
            Ok(Box::new(index_map))
        }

        DataType::LargeBinary => {
            let index_map = BytesLikeIndexMap::<
                GenericBytesHelper<GenericBinaryType<i64>>,
            >::try_from(unique_non_null_scalars)?;
            Ok(Box::new(index_map))
        }

        DataType::FixedSizeBinary(_) => {
            let index_map = BytesLikeIndexMap::<FixedBinaryHelper>::try_from(
                unique_non_null_scalars,
            )?;
            Ok(Box::new(index_map))
        }

        DataType::Utf8View => {
            let index_map =
                BytesLikeIndexMap::<GenericBytesViewHelper<StringViewType>>::try_from(
                    unique_non_null_scalars,
                )?;
            Ok(Box::new(index_map))
        }
        DataType::BinaryView => {
            let index_map =
                BytesLikeIndexMap::<GenericBytesViewHelper<BinaryViewType>>::try_from(
                    unique_non_null_scalars,
                )?;
            Ok(Box::new(index_map))
        }

        DataType::Dictionary(key, value) => {
            macro_rules! downcast_dictionary_array_helper {
                ($t:ty) => {{
                    create_index_map_for_dictionary_input::<$t>(
                        value.as_ref(),
                        unique_non_null_scalars,
                    )
                }};
            }

            downcast_integer! {
                key.as_ref() => (downcast_dictionary_array_helper),
                k => unreachable!("unsupported dictionary key type: {}", k)
            }
        }
        _ => Err(plan_datafusion_err!(
            "Unsupported data type for index map: {}",
            unique_non_null_scalars[0].data_type()
        )),
    }
}

fn create_index_map_for_dictionary_input<K: ArrowDictionaryKeyType + Send + Sync>(
    value: &DataType,
    unique_non_null_scalars: Vec<ScalarValue>,
) -> datafusion_common::Result<Box<dyn ScalarIndexMap>> {
    // TODO - optimize dictionary to use different wrapper that takes advantage of it being a dictionary
    match value {
        DataType::Utf8 => {
            let lookup_table = BytesLikeIndexMap::<
                BytesDictionaryHelper<K, GenericStringType<i32>>,
            >::try_from(unique_non_null_scalars)?;
            Ok(Box::new(lookup_table))
        }

        DataType::LargeUtf8 => {
            let lookup_table = BytesLikeIndexMap::<
                BytesDictionaryHelper<K, GenericStringType<i64>>,
            >::try_from(unique_non_null_scalars)?;
            Ok(Box::new(lookup_table))
        }

        DataType::Binary => {
            let lookup_table = BytesLikeIndexMap::<
                BytesDictionaryHelper<K, GenericBinaryType<i32>>,
            >::try_from(unique_non_null_scalars)?;
            Ok(Box::new(lookup_table))
        }

        DataType::LargeBinary => {
            let lookup_table = BytesLikeIndexMap::<
                BytesDictionaryHelper<K, GenericBinaryType<i64>>,
            >::try_from(unique_non_null_scalars)?;
            Ok(Box::new(lookup_table))
        }

        DataType::FixedSizeBinary(_) => {
            let lookup_table =
                BytesLikeIndexMap::<FixedBytesDictionaryHelper<K>>::try_from(
                    unique_non_null_scalars,
                )?;
            Ok(Box::new(lookup_table))
        }

        DataType::Utf8View => {
            let lookup_table = BytesLikeIndexMap::<
                BytesViewDictionaryHelper<K, StringViewType>,
            >::try_from(unique_non_null_scalars)?;
            Ok(Box::new(lookup_table))
        }
        DataType::BinaryView => {
            let lookup_table = BytesLikeIndexMap::<
                BytesViewDictionaryHelper<K, BinaryViewType>,
            >::try_from(unique_non_null_scalars)?;
            Ok(Box::new(lookup_table))
        }
        _ => Err(plan_datafusion_err!(
            "Unsupported dictionary value type for lookup table: {}",
            value
        )),
    }
}
