#[derive(Default, Debug, Clone, PartialEq)]
pub struct Query {
    selection: Vec<Expr>,
}

impl Query {
    pub fn select(self, selection: Vec<Expr>) -> Query {
        Query { selection, ..self }
    }

    pub fn to_sql(&self, table_name: &str) -> String {
        format!(
            "SELECT {} FROM {}",
            self.selection
                .iter()
                .map(|s| s.to_sql()) // Escape potentially conflicting column names
                .collect::<Vec<String>>()
                .join(", "),
            table_name
        )
    }
}

#[derive(Debug, Clone, PartialEq)]
pub enum Expr {
    Column(String),
    Alias(Box<Expr>, String),
    Literal(Literal),
    Add(Box<Expr>, Box<Expr>),
}

impl Expr {
    pub fn to_sql(&self) -> String {
        match self {
            Expr::Column(name) => "\"".to_string() + name + "\"",
            Expr::Alias(expr, alias) => format!("{} AS {}", expr.to_sql(), alias),
            Expr::Literal(lit) => lit.to_sql(),
            Expr::Add(a, b) => format!("({} + {})", a.to_sql(), b.to_sql()),
        }
    }
}

pub fn col(name: &str) -> Expr {
    Expr::Column(name.to_string())
}

#[derive(Debug, Clone, PartialEq)]
pub enum Literal {
    Null,
    Bool(bool),
    Int(i64),
    Float(f64),
    String(String),
}

impl Literal {
    pub fn to_sql(&self) -> String {
        match self {
            Literal::Null => "NULL".to_string(),
            Literal::Bool(b) => b.to_string(),
            Literal::Int(i) => i.to_string(),
            Literal::Float(f) => f.to_string(),
            Literal::String(s) => "'".to_string() + s + "'",
        }
    }
}
