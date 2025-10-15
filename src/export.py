def write_parquet(df, path, mode='overwrite'):
    df.write.mode(mode).parquet(path)
