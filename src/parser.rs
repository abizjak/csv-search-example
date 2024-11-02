use crate::{ColumnType, CompiledExpr, CompiledFilter, CompiledQuery, Test};
use nom::bytes::complete::tag;
use nom::character::complete::multispace0;
use nom::character::complete::{self as parser, alphanumeric1};
use nom::character::complete::{alpha1, multispace1};
use nom::combinator::{eof, map, recognize};
use nom::error::VerboseError;
use nom::multi::separated_list1;
use nom::sequence::{delimited, pair};
use nom::{Finish, IResult};
use std::collections::HashMap;

/// A parsed query, but not yet resolved fully and ready to execute.
/// Use [`execute_query`](crate::LoadedCSV::execute_query)
#[derive(Debug)]
pub struct Query {
    projections: Vec<String>,
    filters: Vec<Filter>,
}

#[derive(Debug, PartialEq, Eq)]
/// An expression that is part of a filter.
enum Expr {
    Var { column_idx: String },
    Const { val: String },
}

impl Expr {
    /// If the expression is a variable and refers to a known column, return the column it refers to.
    /// Otherwise return Ok(None).
    fn resolve_column(&self, mapping: &HashMap<&str, usize>) -> anyhow::Result<Option<usize>> {
        match self {
            Expr::Var { column_idx } => mapping
                .get(column_idx.as_str())
                .ok_or_else(|| anyhow::anyhow!("Unknown column '{column_idx}'"))
                .copied()
                .map(Some),
            Expr::Const { .. } => Ok(None),
        }
    }

    fn resolve_const(self, column_type: ColumnType) -> anyhow::Result<CompiledExpr> {
        match self {
            Expr::Var { .. } => {
                anyhow::bail!("Expr is expected to be a variable. This is precondition violation.")
            }
            Expr::Const { val } => match column_type {
                ColumnType::String => Ok(CompiledExpr::StringConst { val }),
                ColumnType::Integer => {
                    let val = val.parse::<i64>()?;
                    Ok(CompiledExpr::IntConst { val })
                }
            },
        }
    }
}

#[derive(Debug, PartialEq, Eq)]
/// A single filter.
struct Filter {
    left: Expr,
    right: Expr,
    test: Test,
}

impl Filter {
    /// Compile and validate the filter in the context of the provided schema which supplies
    /// a mapping of column names to their index in the input data, and the types of those colums.
    fn compile(
        self,
        column_types: &[ColumnType],
        mapping: &HashMap<&str, usize>,
    ) -> anyhow::Result<CompiledFilter> {
        let left_var = self.left.resolve_column(mapping)?;
        let right_var = self.right.resolve_column(mapping)?;

        let ty = match (left_var, right_var) {
            (None, None) => {
                anyhow::bail!("both operands are constants. Invalid test.",);
            }
            (None, Some(t)) => column_types[t],
            (Some(t), None) => column_types[t],
            (Some(t1), Some(t2)) => {
                let t1 = column_types[t1];
                let t2 = column_types[t2];
                if t1 == t2 {
                    t1
                } else {
                    anyhow::bail!("operand types inconsistent {t1:?} != {t2:?}",);
                }
            }
        };
        let left = match left_var {
            Some(column_idx) => CompiledExpr::Var { column_idx },
            None => self.left.resolve_const(ty)?,
        };
        let right = match right_var {
            Some(column_idx) => CompiledExpr::Var { column_idx },
            None => self.right.resolve_const(ty)?,
        };
        Ok(CompiledFilter {
            left,
            right,
            ty,
            test: self.test,
        })
    }
}

fn parse_expr(i: &str) -> IResult<&str, Expr, VerboseError<&str>> {
    nom::branch::alt((
        delimited(
            parser::char('"'),
            map(alphanumeric1, |v| Expr::Const {
                val: String::from(v),
            }),
            parser::char('"'),
        ),
        map(alpha1, |v| Expr::Var {
            column_idx: String::from(v),
        }),
    ))(i)
}

fn parse_filter(orig: &str) -> IResult<&str, Filter, VerboseError<&str>> {
    let (i, left) = parse_expr(orig)?;
    let (i, _) = multispace0(i)?;
    let (i, operator) = nom::branch::alt((tag("="), tag(">="), tag(">")))(i)?;
    let (i, _) = multispace0(i)?;
    let (i, right) = parse_expr(i)?;

    let test = match operator {
        "=" => Test::Equal,
        ">=" => Test::GreaterOrEqual,
        ">" => Test::Greater,
        _ => unreachable!("Only three operators supported."),
    };
    Ok((i, Filter { test, left, right }))
}

pub fn parse_query<'a>(ii: &'a str) -> anyhow::Result<Query> {
    let parser = |i: &'a str| -> IResult<(), Query, nom::error::VerboseError<_>> {
        let (i, _) = tag("PROJECT")(i)?;
        let (i, _) = multispace1(i)?;
        let (i, projections) = separated_list1(
            pair(parser::char(','), multispace0),
            map(recognize(alphanumeric1), String::from),
        )(i)?;
        let (i, _) = multispace0(i)?;
        if eof::<_, nom::error::Error<_>>(i).is_ok() {
            return Ok((
                (),
                Query {
                    projections,
                    filters: Vec::new(),
                },
            ));
        };
        let (i, _) = tag("FILTER")(i)?;
        let (i, _) = multispace1(i)?;
        let (i, filters) = separated_list1(pair(parser::char(','), multispace0), parse_filter)(i)?;
        let (rest, _) = multispace0(i)?;
        eof(rest)?;
        Ok((
            (),
            Query {
                projections,
                filters,
            },
        ))
    };
    let result = parser(ii)
        .finish()
        .map_err(|e| anyhow::anyhow!(e.to_string()))?;
    Ok(result.1)
}

impl Query {
    pub(crate) fn compile(
        self,
        column_types: &[ColumnType],
        column_names: &[String],
    ) -> anyhow::Result<CompiledQuery> {
        // TODO: With better modelling we don't need this test.
        anyhow::ensure!(
            column_names.len() == column_types.len(),
            "Column types and names don't match."
        );
        let mut projections = Vec::with_capacity(self.projections.len());
        let column_mapping = column_names
            .iter()
            .map(|s| s.as_str())
            .zip(0..)
            .collect::<HashMap<_, _>>();
        for projection in &self.projections {
            let Some(idx) = column_mapping.get(projection.as_str()) else {
                anyhow::bail!("Unknown column name '{projection}'");
            };
            projections.push((*idx, String::from(projection)));
        }
        let mut filters = Vec::with_capacity(self.filters.len());
        for f in self.filters {
            filters.push(f.compile(column_types, &column_mapping)?);
        }
        Ok(CompiledQuery {
            projections,
            filters,
        })
    }
}

/// Basic tests, for basic functionality.
#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse1() -> anyhow::Result<()> {
        let query = parse_query("PROJECT a")?;
        assert!(query.filters.is_empty());
        assert_eq!(&query.projections, &["a"]);
        Ok(())
    }

    #[test]
    fn test_parse2() {
        assert!(parse_query("PROJECT").is_err());
    }

    #[test]
    fn test_parse3() -> anyhow::Result<()> {
        let query = parse_query("PROJECT a, b FILTER a > \"3\", b = \"4\", c >= \"5\"")?;
        let f1 = Filter {
            left: Expr::Var {
                column_idx: "a".into(),
            },
            right: Expr::Const { val: "3".into() },
            test: Test::Greater,
        };
        let f2 = Filter {
            left: Expr::Var {
                column_idx: "b".into(),
            },
            right: Expr::Const { val: "4".into() },
            test: Test::Equal,
        };
        let f3 = Filter {
            left: Expr::Var {
                column_idx: "c".into(),
            },
            right: Expr::Const { val: "5".into() },
            test: Test::GreaterOrEqual,
        };
        assert_eq!(query.filters, [f1, f2, f3]);
        assert_eq!(query.projections, ["a", "b"]);
        Ok(())
    }

    #[test]
    fn test_compile() -> anyhow::Result<()> {
        let names: [String; 3] = ["a".into(), "b".into(), "c".into()];
        let types: [ColumnType; 3] = [ColumnType::Integer, ColumnType::String, ColumnType::String];

        let f1 = Filter {
            left: Expr::Var {
                column_idx: "a".into(),
            },
            right: Expr::Const { val: "3".into() },
            test: Test::Greater,
        };
        let f2 = Filter {
            left: Expr::Var {
                column_idx: "b".into(),
            },
            right: Expr::Const { val: "4".into() },
            test: Test::Equal,
        };
        let f3 = Filter {
            left: Expr::Var {
                column_idx: "c".into(),
            },
            right: Expr::Const { val: "5".into() },
            test: Test::GreaterOrEqual,
        };
        let query = Query {
            projections: vec!["a".into(), "b".into()],
            filters: vec![f1, f2, f3],
        };
        query.compile(&types, &names)?;
        Ok(())
    }
}
