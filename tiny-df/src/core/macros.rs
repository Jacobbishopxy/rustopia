//! Tiny DF Macro

/// generate Series
#[macro_export]
macro_rules! d1 {
    [$($x:expr),* $(,)*] => {
        {
            let mut buf_vec: Vec<DataframeData> = Vec::new();
            $(
                buf_vec.push($x.into());
            )*
            buf_vec
        } as Vec<DataframeData>
    };
}

#[macro_export]
macro_rules! series {
    [$name:expr => [$($x:expr),* $(,)*]] => {
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
macro_rules! d2 {
    [$([$($x:expr),* $(,)*]),+ $(,)*] => {
        vec![
            $(
                {
                    let mut buf_vec: Vec<DataframeData> = Vec::new();
                    $(
                        buf_vec.push($x.into());
                    )*
                    buf_vec
                },
            )*
        ] as Vec<Vec<DataframeData>>
    };
}

/// generate Dataframe
#[macro_export]
macro_rules! df {
    [$orient:expr; $($name:expr => [$($x:expr),* $(,)*]),+ $(,)*] => {
        {
            let mut buf = vec![];
            $(
                {
                    let mut tmp = vec![];
                    $(
                        tmp.push($x.into());
                    )*
                    buf.push(Series::new($name.into(), tmp));
                }
            )+
            DataFrame::from_series(buf, $orient)
        }
    }
}
