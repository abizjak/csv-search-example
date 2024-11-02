use clap::Parser;
use csv_search::{parse_query, LoadedCSV};

#[derive(Parser, Debug)]
#[command(version, about)]
struct Args {
    /// Input file to process.
    #[arg(long)]
    input: std::path::PathBuf,
    /// Query to run.
    #[arg(long)]
    query: String,
}

fn main() -> anyhow::Result<()> {
    let args = Args::parse();

    let data = LoadedCSV::from_path(args.input)?;
    let query = parse_query(&args.query)?;
    let query_output = data.execute_query(query)?;

    // Output filtered rows.
    let out_writer = std::io::stdout().lock();
    let mut writer = csv::WriterBuilder::new().from_writer(out_writer);
    writer.write_record(&query_output.headers)?;
    for out in query_output {
        writer.write_record(out)?;
    }
    writer.flush()?;
    Ok(())
}
