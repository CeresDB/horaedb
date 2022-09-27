// Copyright 2022 CeresDB Project Authors. Licensed under Apache-2.0.

use std::{mem, sync::Arc};

use arrow_deps::{
    arrow::{compute, compute::kernels::cast_utils::string_to_timestamp_nanos},
    datafusion::{
        arrow::datatypes::DataType,
        error::{DataFusionError, Result},
        logical_plan::{
            plan::Filter, DFSchemaRef, Expr, ExprRewritable, ExprRewriter, LogicalPlan, Operator,
            TableScan,
        },
        optimizer::{optimizer::OptimizerRule, OptimizerConfig},
        scalar::ScalarValue,
    },
    datafusion_expr::{utils, ExprSchemable},
};
use log::debug;

/// Optimizer that cast literal value to target column's type
///
/// Example transformations that are applied:
/// * `expr > '5'` to `expr > 5` when `expr` is of numeric type
/// * `expr > '2021-12-02 15:00:34'` to `expr > 1638428434000(ms)` when `expr`
///   is of timestamp type
/// * `expr > 10` to `expr > '10'` when `expr` is of string type
/// * `expr = 'true'` to `expr = true` when `expr` is of boolean type
pub struct TypeConversion;

impl OptimizerRule for TypeConversion {
    fn optimize(
        &self,
        plan: &LogicalPlan,
        optimizer_config: &mut OptimizerConfig,
    ) -> Result<LogicalPlan> {
        let mut rewriter = TypeRewriter {
            schemas: plan.all_schemas(),
        };

        match plan {
            LogicalPlan::Filter(Filter { predicate, input }) => Ok(LogicalPlan::Filter(Filter {
                predicate: predicate.clone().rewrite(&mut rewriter)?,
                input: Arc::new(self.optimize(input, optimizer_config)?),
            })),
            LogicalPlan::TableScan(TableScan {
                table_name,
                source,
                projection,
                projected_schema,
                filters,
                fetch,
            }) => {
                let rewrite_filters = filters
                    .clone()
                    .into_iter()
                    .map(|e| e.rewrite(&mut rewriter))
                    .collect::<Result<Vec<_>>>()?;
                Ok(LogicalPlan::TableScan(TableScan {
                    table_name: table_name.clone(),
                    source: source.clone(),
                    projection: projection.clone(),
                    projected_schema: projected_schema.clone(),
                    filters: rewrite_filters,
                    fetch: *fetch,
                }))
            }
            LogicalPlan::Projection { .. }
            | LogicalPlan::Window { .. }
            | LogicalPlan::Aggregate { .. }
            | LogicalPlan::Repartition { .. }
            | LogicalPlan::CreateExternalTable { .. }
            | LogicalPlan::Extension { .. }
            | LogicalPlan::Sort { .. }
            | LogicalPlan::Explain { .. }
            | LogicalPlan::Limit { .. }
            | LogicalPlan::Union { .. }
            | LogicalPlan::Join { .. }
            | LogicalPlan::CrossJoin { .. }
            | LogicalPlan::CreateMemoryTable { .. }
            | LogicalPlan::DropTable { .. }
            | LogicalPlan::DropView { .. }
            | LogicalPlan::Values { .. }
            | LogicalPlan::Analyze { .. }
            | LogicalPlan::Distinct { .. } => {
                let inputs = plan.inputs();
                let new_inputs = inputs
                    .iter()
                    .map(|plan| self.optimize(plan, optimizer_config))
                    .collect::<Result<Vec<_>>>()?;

                let expr = plan
                    .expressions()
                    .into_iter()
                    .map(|e| e.rewrite(&mut rewriter))
                    .collect::<Result<Vec<_>>>()?;

                utils::from_plan(plan, &expr, &new_inputs)
            }

            LogicalPlan::Subquery(_)
            | LogicalPlan::SubqueryAlias(_)
            | LogicalPlan::CreateView(_)
            | LogicalPlan::CreateCatalogSchema(_)
            | LogicalPlan::CreateCatalog(_)
            | LogicalPlan::EmptyRelation { .. } => Ok(plan.clone()),
        }
    }

    fn name(&self) -> &str {
        "type_conversion"
    }
}

struct TypeRewriter<'a> {
    /// input schemas
    schemas: Vec<&'a DFSchemaRef>,
}

impl<'a> TypeRewriter<'a> {
    fn column_data_type(&self, expr: &Expr) -> Option<DataType> {
        if let Expr::Column(_) = expr {
            for schema in &self.schemas {
                if let Ok(v) = expr.get_type(schema) {
                    return Some(v);
                }
            }
        }

        None
    }

    fn convert_type<'b>(&self, mut left: &'b Expr, mut right: &'b Expr) -> Result<(Expr, Expr)> {
        let left_type = self.column_data_type(left);
        let right_type = self.column_data_type(right);

        let mut reverse = false;
        let left_type = match (&left_type, &right_type) {
            (Some(v), None) => v,
            (None, Some(v)) => {
                reverse = true;
                mem::swap(&mut left, &mut right);
                v
            }
            _ => return Ok((left.clone(), right.clone())),
        };

        match (left, right) {
            (Expr::Column(col), Expr::Literal(value)) => {
                let casted_right = Self::cast_scalar_value(value, left_type)?;
                debug!(
                    "TypeRewriter convert type, origin_left:{:?}, type:{}, right:{:?}, casted_right:{:?}",
                    col, left_type, value, casted_right
                );
                if casted_right.is_null() {
                    return Err(DataFusionError::Plan(format!(
                        "column:{:?} value:{:?} is invalid",
                        col, value
                    )));
                }
                if reverse {
                    Ok((Expr::Literal(casted_right), left.clone()))
                } else {
                    Ok((left.clone(), Expr::Literal(casted_right)))
                }
            }
            _ => Ok((left.clone(), right.clone())),
        }
    }

    fn cast_scalar_value(value: &ScalarValue, data_type: &DataType) -> Result<ScalarValue> {
        if let DataType::Timestamp(_, _) = data_type {
            if let ScalarValue::Utf8(Some(v)) = value {
                return string_to_timestamp_ms(v);
            }
        }

        if let DataType::Boolean = data_type {
            if let ScalarValue::Utf8(Some(v)) = value {
                return match v.to_lowercase().as_str() {
                    "true" => Ok(ScalarValue::Boolean(Some(true))),
                    "false" => Ok(ScalarValue::Boolean(Some(false))),
                    _ => Ok(ScalarValue::Boolean(None)),
                };
            }
        }

        let array = value.to_array();
        ScalarValue::try_from_array(
            &compute::cast(&array, data_type).map_err(DataFusionError::ArrowError)?,
            // index: Converts a value in `array` at `index` into a ScalarValue
            0,
        )
    }
}

impl<'a> ExprRewriter for TypeRewriter<'a> {
    fn mutate(&mut self, expr: Expr) -> Result<Expr> {
        let new_expr = match expr {
            Expr::BinaryExpr { left, op, right } => match op {
                Operator::Eq
                | Operator::NotEq
                | Operator::Lt
                | Operator::LtEq
                | Operator::Gt
                | Operator::GtEq => {
                    let (left, right) = self.convert_type(&left, &right)?;
                    Expr::BinaryExpr {
                        left: Box::new(left),
                        op,
                        right: Box::new(right),
                    }
                }
                _ => Expr::BinaryExpr { left, op, right },
            },
            Expr::Between {
                expr,
                negated,
                low,
                high,
            } => {
                let (expr, low) = self.convert_type(&expr, &low)?;
                let (expr, high) = self.convert_type(&expr, &high)?;
                Expr::Between {
                    expr: Box::new(expr),
                    negated,
                    low: Box::new(low),
                    high: Box::new(high),
                }
            }
            Expr::InList {
                expr,
                list,
                negated,
            } => {
                let mut list_expr = Vec::with_capacity(list.len());
                for e in list {
                    let (_, expr_conversion) = self.convert_type(&expr, &e)?;
                    list_expr.push(expr_conversion);
                }
                Expr::InList {
                    expr,
                    list: list_expr,
                    negated,
                }
            }
            Expr::Literal(value) => match value {
                ScalarValue::TimestampSecond(Some(i), _) => {
                    timestamp_to_timestamp_ms_expr(TimestampType::Second, i)
                }
                ScalarValue::TimestampMicrosecond(Some(i), _) => {
                    timestamp_to_timestamp_ms_expr(TimestampType::Microsecond, i)
                }
                ScalarValue::TimestampNanosecond(Some(i), _) => {
                    timestamp_to_timestamp_ms_expr(TimestampType::Nanosecond, i)
                }
                _ => Expr::Literal(value),
            },
            expr => {
                // no rewrite possible
                expr
            }
        };
        Ok(new_expr)
    }
}

fn string_to_timestamp_ms(string: &str) -> Result<ScalarValue> {
    Ok(ScalarValue::TimestampMillisecond(
        Some(
            string_to_timestamp_nanos(string)
                .map(|t| t / 1_000_000)
                .map_err(DataFusionError::from)?,
        ),
        None,
    ))
}

enum TimestampType {
    Second,
    #[allow(dead_code)]
    Millisecond,
    Microsecond,
    Nanosecond,
}

fn timestamp_to_timestamp_ms_expr(typ: TimestampType, timestamp: i64) -> Expr {
    let timestamp = match typ {
        TimestampType::Second => timestamp * 1_000,
        TimestampType::Millisecond => timestamp,
        TimestampType::Microsecond => timestamp / 1_000,
        TimestampType::Nanosecond => timestamp / 1_000 / 1_000,
    };

    Expr::Literal(ScalarValue::TimestampMillisecond(Some(timestamp), None))
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use arrow_deps::{
        arrow::datatypes::TimeUnit,
        datafusion::{
            logical_plan::{DFField, DFSchema},
            prelude::col,
        },
    };

    use super::*;

    fn expr_test_schema() -> DFSchemaRef {
        Arc::new(
            DFSchema::new_with_metadata(
                vec![
                    DFField::new(None, "c1", DataType::Utf8, true),
                    DFField::new(None, "c2", DataType::Int64, true),
                    DFField::new(None, "c3", DataType::Float64, true),
                    DFField::new(None, "c4", DataType::Float32, true),
                    DFField::new(None, "c5", DataType::Boolean, true),
                    DFField::new(
                        None,
                        "c6",
                        DataType::Timestamp(TimeUnit::Millisecond, None),
                        false,
                    ),
                ],
                HashMap::new(),
            )
            .unwrap(),
        )
    }

    #[test]
    fn test_type_conversion_int64() {
        let int_value = 100;
        let int_str = int_value.to_string();
        let not_int_str = "100ss".to_string();
        let schema = expr_test_schema();
        let mut rewriter = TypeRewriter {
            schemas: vec![&schema],
        };

        // Int64 c2 > "100" success
        let exp = col("c2").gt(Expr::Literal(ScalarValue::Utf8(Some(int_str.clone()))));
        let rewrite_exp = exp.rewrite(&mut rewriter).unwrap();
        assert_eq!(
            rewrite_exp,
            col("c2").gt(Expr::Literal(ScalarValue::Int64(Some(int_value)),))
        );

        // Int64 "100" > c2 success
        let exp = Expr::Literal(ScalarValue::Utf8(Some(int_str))).gt(col("c2"));
        let rewrite_exp = exp.rewrite(&mut rewriter).unwrap();
        assert_eq!(
            rewrite_exp,
            Expr::Literal(ScalarValue::Int64(Some(int_value))).gt(col("c2"))
        );

        // Int64 c2 > "100ss" fail
        let exp = col("c2").gt(Expr::Literal(ScalarValue::Utf8(Some(not_int_str))));
        assert!(exp.rewrite(&mut rewriter).is_err());
    }

    #[test]
    fn test_type_conversion_float() {
        let double_value = 100.1;
        let double_str = double_value.to_string();
        let not_int_str = "100ss".to_string();
        let schema = expr_test_schema();
        let mut rewriter = TypeRewriter {
            schemas: vec![&schema],
        };

        // Float64 c3 > "100" success
        let exp = col("c3").gt(Expr::Literal(ScalarValue::Utf8(Some(double_str.clone()))));
        let rewrite_exp = exp.rewrite(&mut rewriter).unwrap();
        assert_eq!(
            rewrite_exp,
            col("c3").gt(Expr::Literal(ScalarValue::Float64(Some(double_value)),))
        );

        // Float64 c3 > "100ss" fail
        let exp = col("c3").gt(Expr::Literal(ScalarValue::Utf8(Some(not_int_str.clone()))));
        assert!(exp.rewrite(&mut rewriter).is_err());

        // Float32 c4 > "100" success
        let exp = col("c4").gt(Expr::Literal(ScalarValue::Utf8(Some(double_str))));
        let rewrite_exp = exp.rewrite(&mut rewriter).unwrap();
        assert_eq!(
            rewrite_exp,
            col("c4").gt(Expr::Literal(ScalarValue::Float32(Some(
                double_value as f32
            )),))
        );

        // Float32 c4 > "100ss" fail
        let exp = col("c4").gt(Expr::Literal(ScalarValue::Utf8(Some(not_int_str))));
        assert!(exp.rewrite(&mut rewriter).is_err());
    }

    #[test]
    fn test_type_conversion_boolean() {
        let bool_value = true;
        let bool_str = bool_value.to_string();
        let not_int_str = "100ss".to_string();
        let schema = expr_test_schema();
        let mut rewriter = TypeRewriter {
            schemas: vec![&schema],
        };

        // Boolean c5 > "100ss" fail
        let exp = col("c5").gt(Expr::Literal(ScalarValue::Utf8(Some(not_int_str))));
        assert!(exp.rewrite(&mut rewriter).is_err());

        // Boolean c5 > "true" success
        let exp = col("c5").gt(Expr::Literal(ScalarValue::Utf8(Some(bool_str))));
        let rewrite_exp = exp.rewrite(&mut rewriter).unwrap();
        assert_eq!(
            rewrite_exp,
            col("c5").gt(Expr::Literal(ScalarValue::Boolean(Some(bool_value)),))
        );

        // Boolean c5 > true success
        let exp = col("c5").gt(Expr::Literal(ScalarValue::Boolean(Some(bool_value))));
        let rewrite_exp = exp.rewrite(&mut rewriter).unwrap();
        assert_eq!(
            rewrite_exp,
            col("c5").gt(Expr::Literal(ScalarValue::Boolean(Some(bool_value)),))
        );
    }

    #[test]
    fn test_type_conversion_timestamp() {
        let date_string = "2021-09-07 16:00:00".to_string();
        let schema = expr_test_schema();
        let mut rewriter = TypeRewriter {
            schemas: vec![&schema],
        };

        // Timestamp c6 > "2021-09-07 16:00:00"
        let exp = col("c6").gt(Expr::Literal(ScalarValue::Utf8(Some(date_string.clone()))));
        let rewrite_exp = exp.rewrite(&mut rewriter).unwrap();
        assert_eq!(
            rewrite_exp,
            col("c6").gt(Expr::Literal(ScalarValue::TimestampMillisecond(
                Some(
                    string_to_timestamp_nanos(&date_string)
                        .map(|t| t / 1_000_000)
                        .unwrap(),
                ),
                None
            ),))
        );

        // "2021-09-07 16:00:00" > Timestamp c6
        let exp = Expr::Literal(ScalarValue::Utf8(Some(date_string.clone()))).gt(col("c6"));
        let rewrite_exp = exp.rewrite(&mut rewriter).unwrap();
        assert_eq!(
            rewrite_exp,
            Expr::Literal(ScalarValue::TimestampMillisecond(
                Some(
                    string_to_timestamp_nanos(&date_string)
                        .map(|t| t / 1_000_000)
                        .unwrap(),
                ),
                None
            ),)
            .gt(col("c6"))
        );

        // Timestamp c6 > 1642141472
        let timestamp_int = 1642141472;
        let exp = col("c6").gt(Expr::Literal(ScalarValue::TimestampSecond(
            Some(timestamp_int),
            None,
        )));
        let rewrite_exp = exp.rewrite(&mut rewriter).unwrap();
        assert_eq!(
            rewrite_exp,
            col("c6").gt(Expr::Literal(ScalarValue::TimestampMillisecond(
                Some(timestamp_int * 1000),
                None
            )))
        );

        // Timestamp c6 between "2021-09-07 16:00:00" and "2021-09-07 17:00:00"
        let date_string2 = "2021-09-07 17:00:00".to_string();
        let exp = Expr::Between {
            expr: Box::new(col("c6")),
            negated: false,
            low: Box::new(Expr::Literal(ScalarValue::Utf8(Some(date_string.clone())))),
            high: Box::new(Expr::Literal(ScalarValue::Utf8(Some(date_string2.clone())))),
        };
        let rewrite_exp = exp.rewrite(&mut rewriter).unwrap();
        assert_eq!(
            rewrite_exp,
            Expr::Between {
                expr: Box::new(col("c6")),
                negated: false,
                low: Box::new(Expr::Literal(ScalarValue::TimestampMillisecond(
                    Some(
                        string_to_timestamp_nanos(&date_string)
                            .map(|t| t / 1_000_000)
                            .unwrap(),
                    ),
                    None
                ),)),
                high: Box::new(Expr::Literal(ScalarValue::TimestampMillisecond(
                    Some(
                        string_to_timestamp_nanos(&date_string2)
                            .map(|t| t / 1_000_000)
                            .unwrap(),
                    ),
                    None
                ),))
            }
        );
    }
}
