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

//! Simplify `AND` and `OR` expressions in physical expressions
//!
//! This module provides optimizations for `AND` and `OR` expressions such as:
//! - Constant folding: x AND TRUE -> x, x AND FALSE -> FALSE, x OR TRUE -> TRUE, x OR FALSE -> x
//!
//! This function is designed to work with TreeNodeRewriter's f_up traversal,
//! which means children are already simplified when this function is called.
//! The TreeNodeRewriter will automatically call this function repeatedly until
//! no more transformations are possible.

use std::sync::Arc;

use datafusion_common::{tree_node::Transformed, Result, ScalarValue};
use datafusion_expr::Operator;

use crate::expressions::{BinaryExpr, Literal};
use crate::PhysicalExpr;

/// Attempts to simplify `AND` and `OR` expressions by applying one level of transformation
///
/// This function applies a single simplification rule and returns. When used with
/// TreeNodeRewriter, multiple passes will automatically be applied until no more
/// transformations are possible.
pub fn simplify_and_or_expr(
    expr: &Arc<dyn PhysicalExpr>,
) -> Result<Transformed<Arc<dyn PhysicalExpr>>> {
    let simplified = match expr.as_any().downcast_ref::<BinaryExpr>() {
        Some(b) if b.op().eq(&Operator::And) => {
            match (as_bool(b.left()), as_bool(b.right())) {
                // x AND false = false
                (Some(false), _) => Some(Arc::clone(b.left())),
                (_, Some(false)) => Some(Arc::clone(b.right())),
                // x AND false = x
                (Some(true), _) => Some(Arc::clone(b.right())),
                (_, Some(true)) => Some(Arc::clone(b.left())),
                _ => None,
            }
        },
        Some(b) if b.op().eq(&Operator::Or) => {
            match (as_bool(b.left()), as_bool(b.right())) {
                // x OR true = true
                (Some(true), _) => Some(Arc::clone(b.left())),
                (_, Some(true)) => Some(Arc::clone(b.right())),
                // x OR false = x
                (Some(false), _) => Some(Arc::clone(b.right())),
                (_, Some(false)) => Some(Arc::clone(b.left())),
                _ => None,
            }
        },
        _ => None,
    };

    Ok(match simplified {
        None => Transformed::no(Arc::clone(expr)),
        Some(e) => Transformed::yes(e),
    })
}

fn as_bool(expr: &Arc<dyn PhysicalExpr>) -> Option<bool> {
    match expr.as_any().downcast_ref::<Literal>() {
        Some(l) => {
            match l.value() {
                ScalarValue::Boolean(b) => b.clone(),
                _ => None,
            }
        },
        _ => None,
    }
}