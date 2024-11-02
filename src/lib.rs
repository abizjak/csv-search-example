//! A simple CSV searcher that can test ordering and equality on the columns.
//! The main entrypoints to use are
//!
//! - [`LoadedCSV`] and its methods to load CSV into memory
//! - [`Query`]. Use `parse_query` to parse a query from a string.
//!   Then use `execute_query` method of `LoadedCSV` to get the iterator over the output records.

mod parser;

use anyhow::Context as _;
use csv::StringRecord;
pub use parser::{parse_query, Query};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ColumnType {
    String,
    Integer, // For simplicity this means i64.
}

#[derive(Debug)]
pub struct Rows {
    pub types: Vec<ColumnType>,
    rows: Vec<csv::StringRecord>,
}

impl Rows {
    pub fn push(&mut self, row: csv::StringRecord) {
        for (i, ty) in self.types.iter_mut().enumerate() {
            let col = row.get(i).unwrap(); // TODO
            *ty = match *ty {
                ColumnType::Integer if col.parse::<i64>().is_ok() => ColumnType::Integer,
                _ => ColumnType::String,
            };
        }
        self.rows.push(row);
    }

    pub fn empty(num_columns: usize) -> Self {
        Self {
            rows: Vec::new(),
            types: vec![ColumnType::Integer; num_columns],
        }
    }
}

/// A loaded CSV file.
pub struct LoadedCSV {
    pub column_names: Vec<String>,
    pub rows: Rows,
}

impl LoadedCSV {
    /// Read CSV data from the supplied reader. The reader does not have to be buffered,
    /// buffering is done internally.
    pub fn from_reader(reader: impl std::io::Read) -> anyhow::Result<LoadedCSV> {
        let mut reader = csv::ReaderBuilder::new()
            .has_headers(true)
            .flexible(false)
            .from_reader(reader);
        let column_header = reader
            .headers()
            .context("Invalid CSV input with no header row.")?
            .into_iter()
            .map(String::from)
            .collect::<Vec<String>>();
        let mut rows = Rows::empty(column_header.len());
        for row in reader.records() {
            let row = row.context("Unable to parse row.")?;
            rows.push(row);
        }
        Ok(LoadedCSV {
            column_names: column_header,
            rows,
        })
    }

    /// Read CSV file.
    pub fn from_path(path: std::path::PathBuf) -> anyhow::Result<LoadedCSV> {
        let file = std::fs::File::open(path)?; // csv reader is buffered automatically, no need to buffer
        Self::from_reader(file)
    }

    /// Attempt to validate the query on the data, and then execute it.
    /// Validation can fail, and in that case an error will be returned.
    ///
    /// Otherwise an iterator over the output
    pub fn execute_query(&self, query: Query) -> anyhow::Result<QueryOutput<'_>> {
        let compiled_query = query.compile(&self.rows.types, &self.column_names)?;
        let headers = compiled_query.out_header();
        Ok(QueryOutput {
            compiled_query,
            headers,
            iter: self.rows.rows.iter(),
        })
    }
}

pub struct QueryOutput<'a> {
    compiled_query: CompiledQuery,
    pub headers: Vec<String>,
    iter: std::slice::Iter<'a, StringRecord>,
}

impl<'a> Iterator for QueryOutput<'a> {
    type Item = Vec<&'a str>;

    fn next(&mut self) -> Option<Self::Item> {
        for record in self.iter.by_ref() {
            let out = self.compiled_query.on_row(record);
            if out.is_some() {
                return out;
            }
            // else try the next one, or terminate.
        }
        None
    }
}

#[derive(Debug)]
enum CompiledExpr {
    Var { column_idx: usize },
    IntConst { val: i64 },
    StringConst { val: String },
}

impl CompiledExpr {
    /// Get an integer out of the expression. This function assumes
    /// that either the expression is an int constant, or that can be parsed.
    ///
    /// This precondition is meant to be ensured by validation/compilation of the schema.
    /// If the precondition is violated this method will panic.
    fn get_int(&self, ctx: &StringRecord) -> i64 {
        match self {
            CompiledExpr::Var { column_idx } => {
                ctx.get(*column_idx).unwrap().parse::<i64>().unwrap()
            }
            CompiledExpr::IntConst { val } => *val,
            CompiledExpr::StringConst { .. } => {
                panic!("Precondition violation. Got string constant but asking for an int.")
            }
        }
    }

    /// Get an integer out of the expression. This function assumes
    /// that either the expression is an int constant, or that can be parsed.
    ///
    /// This precondition is meant to be ensured by validation/compilation of the schema.
    /// If the precondition is violated this method will panic.
    fn get_str<'a>(&'a self, ctx: &'a StringRecord) -> &'a str {
        match self {
            CompiledExpr::Var { column_idx } => ctx.get(*column_idx).unwrap(),
            CompiledExpr::IntConst { .. } => {
                panic!("Precondition violation. Got int constant but asking for a string.")
            }
            CompiledExpr::StringConst { val } => val,
        }
    }
}

#[derive(Debug, Copy, Clone, PartialEq, Eq)]
enum Test {
    Equal,
    Greater,
    GreaterOrEqual,
}

impl Test {
    pub fn test<A: PartialOrd>(self, left: A, right: A) -> bool {
        match self {
            Test::Equal => left == right,
            Test::Greater => left > right,
            Test::GreaterOrEqual => left >= right,
        }
    }
}

#[derive(Debug)]
struct CompiledFilter {
    left: CompiledExpr,
    right: CompiledExpr,
    ty: ColumnType,
    test: Test,
}

/// A query processed in the context of a schema, and ready to execute.
#[derive(Debug)]
struct CompiledQuery {
    projections: Vec<(usize, String)>,
    filters: Vec<CompiledFilter>,
}

impl CompiledFilter {
    fn check_record(&self, row: &StringRecord) -> bool {
        match self.ty {
            ColumnType::String => {
                let l = self.left.get_str(row);
                let r = self.right.get_str(row);
                self.test.test(l, r)
            }
            ColumnType::Integer => {
                let l = self.left.get_int(row);
                let r = self.right.get_int(row);
                self.test.test(l, r)
            }
        }
    }
}

impl CompiledQuery {
    /// Evaluate the compiled query on the given row and output
    /// a row if the filter matches.
    ///
    /// This assumes that the record belongs to the data on which the query was compiled,
    /// otherwise the behaviour is not well-defined and this method might panic.
    fn on_row<'a>(&self, record: &'a StringRecord) -> Option<Vec<&'a str>> {
        if self.filters.iter().all(|filter| filter.check_record(record)) {
            Some(
                self.projections
                    .iter()
                    .map(|i| &record[i.0])
                    .collect::<Vec<_>>(),
            )
        } else {
            None
        }
    }

    fn out_header(&self) -> Vec<String> {
        self.projections.iter().map(|c| c.1.clone()).collect()
    }
}
