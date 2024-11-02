## A simple CSV filtering tool

A simple CSV filtering tool that supports executing simple queries of the form

```
PROJECT col1, col2, ... FILTER col1 > col2, col1 = "3", col3 >= "4"
```

The tool supports i64 or strings as types in the column. A column is deemed of
`i64` type if all the rows can be parsed as such (note that this includes
whitespace, so a column with ` 123` is not considered an integer currently). The
type of the column determines the behaviour of comparison operators, e.g.,
`"123" > "1111"` as strings, but not as integers.

The tool is designed to be memory efficient, and only does two passes of the
data to execute queries. This is also the minimum necessary with the semantics
described above, because we cannot start executing filters until we know the
type of the column in general. Note that this semantics is a bit different than
that implemented by, e.g., Excel. There Strings are always larger than integers.

This is a simple example, and has the following tradeoffs
- the library just uses anyhow::Error everywhere for error reporting, it does
  not define explicit errors (and use, e.g., thiserror to make them ergonomic)
  that can be matched programmatically. This is done because the primary case
  for now is use in the binary.
- only two types are supported, and integers need to fit into i64 for
  simplicity. This is easily extendable with more work.
- parsing is super simple and error reporting is not very good, the underlying
  error from the `nom` parser often leaking to the user. With more effort that
  should be improved so that the error is in terms of terms understandable by
  the user.

Optimizations:
- Currently all the data is loaded into memory, however this is not necessary.
  Two passes over the data are necessary, but we can just stream it twice
  through, the first time to determine the schema/types of columns, and then the
  second time to actually execute the queries. Each of these passes only needs
  constant memory and most of the tool is already built to support this use,
  only small changes are needed.

Other data types:
- supporting other data types is relatively straight forward. We need
  - a method to determine the type of a string value in some deterministic way.
    For example adding a RFC 3339 datetime support is simple since there is no
    overlap with values of this type and integers.
  - use this method during the first pass of the data (in `Rows::push`) to
    determine the type
  - implement the relevant operations on this type in
    `CompiledFilter::check_record` and it's dependencies.

- currently filters can be comparing two columns of the same type, or a column
  to a constant. Comparing two constants is not supported.

- Adding support for sorting of the result would require changing the `on_row`
  method a bit so it would retain typing information of the fields, instead of
  converting them to `&str`, so that we could sort according to the semantics of
  the column type. It would also mean that memory usage of the tool would be
  forced to be linear in the size of the result, unless we did on-disk sorting.

- To support extremely large datasets, as mentioned already above, we'd need to
  not keep the data in memory. Instead we'd make two passes, first to determine
  the column types, and then to filter the columns. If sorting is needed we'd
  need to keep the result in-memory, or do more complex on-disk sorting, which
  would be quite a bit more complex, but it would be essentially a third pass
  to do, e.g., merge-sort with smaller segments on disk.

To make this more production ready at least the following is needed
- make the parser more robust, and make the error reporting more user-friendly
rather than forwarding `nom` errors. Likely writing a recursive descent parser
is a better way to do this.

- more tests of negative scenarios. In the interest of time there is limited
  testing at the moment, only making sure the behaviour is correct in expected
  cases.


## Examples

There are some example input files in the `data`.

To compile the tool run
```
cargo build --release
```

This will produce a single binary `target/release/csv-search`.

The following examples work in UNIX shells. Quoting might have to be adjusted on Windows.

```console
$ ./target/release/csv-search --input "data/input1.csv" --query 'PROJECT aaaa, aaaa FILTER aaaa > "0", cccc >= aaaa'
aaaa,aaaa
1,1
2,2
```

In the following example the `bbbb` column is treated as strings, so `33` is not
greater than `4`.
```console
$ ./target/release/csv-search --input "data/input2.csv" --query 'PROJECT aaaa, bbbb FILTER bbbb >= "4"'
aaaa,bbbb
2,yyy
```
