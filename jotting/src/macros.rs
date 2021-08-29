#[macro_export]
macro_rules! ds {
    ($name:expr => [$($x:expr),* $(,)*]) => {
        {
            let mut buf: D1 = vec![];
            $(
                buf.push($x.into());
            )*
            Series::new($name.into(), buf)
        }
    };
}

/// generate Dataframe
#[macro_export]
macro_rules! df {
    ($($name:expr => $slice:expr), +) => {
        {
            let mut columns = vec![];
            $(
                columns.push(Series::new($name, $slice));
            )+
            DataFrame::new(columns)
        }
    }
}
