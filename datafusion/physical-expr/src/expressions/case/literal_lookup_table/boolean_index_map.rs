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

use crate::expressions::case::literal_lookup_table::ScalarIndexMap;
use arrow::array::{ArrayRef, AsArray};
use datafusion_common::{internal_err, DataFusionError, ScalarValue};

#[derive(Clone, Debug)]
pub(super) struct BooleanIndexMap {
    true_index: Option<u32>,
    false_index: Option<u32>,
}

impl TryFrom<Vec<ScalarValue>> for BooleanIndexMap {
    type Error = DataFusionError;

    /// Try creating a new lookup table from the given scalars
    /// The index of each scalar in the vector is used as the mapped value in the lookup table.
    ///
    /// `unique_non_null_scalars` are guaranteed to be unique and non-nullable
    fn try_from(unique_non_null_scalars: Vec<ScalarValue>) -> Result<Self, Self::Error> {
        let mut true_index: Option<u32> = None;
        let mut false_index: Option<u32> = None;

        for (index, scalar) in unique_non_null_scalars.into_iter().enumerate() {
            match scalar {
                ScalarValue::Boolean(Some(true)) => {
                    if true_index.is_some() {
                        return internal_err!(
                            "Duplicate true value found in values for BooleanIndexMap"
                        );
                    }
                    true_index = Some(index as u32);
                }
                ScalarValue::Boolean(Some(false)) => {
                    if false_index.is_some() {
                        return internal_err!(
                            "Duplicate false value found in values for BooleanIndexMap"
                        );
                    }
                    false_index = Some(index as u32);
                }
                ScalarValue::Boolean(None) => {
                    return internal_err!(
                        "Null value found in non-null values for BooleanIndexMap"
                    )
                }
                _ => {
                    return internal_err!(
                        "Non-boolean value found in values for BooleanIndexMap"
                    )
                }
            }
        }

        Ok(Self {
            true_index,
            false_index,
        })
    }
}

impl ScalarIndexMap for BooleanIndexMap {
    fn map_to_indices(
        &self,
        array: &ArrayRef,
        default_index: u32,
    ) -> datafusion_common::Result<Vec<u32>> {
        let true_index = self.true_index.unwrap_or(default_index);
        let false_index = self.false_index.unwrap_or(default_index);

        Ok(array
            .as_boolean()
            .into_iter()
            .map(|value| match value {
                Some(true) => true_index,
                Some(false) => false_index,
                None => default_index,
            })
            .collect::<Vec<u32>>())
    }
}
