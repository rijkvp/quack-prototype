use quack_prototype::prelude::*;

#[test]
fn test_query() {
    let conn = DuckConnection::open_in_memory();
    let df = Dataframe::open_parquet("prices.parquet", &conn).unwrap();
    assert_eq!(df.meta().unwrap().len(), 3);
    let subset = df.select(&["when", "price"]).unwrap();
    assert_eq!(
        subset.meta().unwrap(),
        vec![
            ("when".to_string(), "TIMESTAMP".to_string()),
            ("price".to_string(), "BIGINT".to_string())
        ]
    );
}

#[test]
fn test_query_lazy() {
    let _ = env_logger::builder()
        .filter_level(log::LevelFilter::Info)
        .is_test(true)
        .init();

    let conn = DuckConnection::open_in_memory();
    let df = Dataframe::open_parquet("prices.parquet", &conn).unwrap();
    let one_column = df
        .lazy()
        .select([col("when"), col("price")])
        .select(&[col("when")])
        .collect()
        .unwrap();
    assert_eq!(
        one_column.meta().unwrap(),
        vec![("when".to_string(), "TIMESTAMP".to_string()),]
    );

    let _expr_column = df
        .lazy()
        .select([Expr::Add(
            Box::new(col("price")),
            Box::new(Expr::Literal(Literal::Int(1))),
        )])
        .collect()
        .unwrap();
}
