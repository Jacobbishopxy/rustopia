//! Tiny DF Macro

/// generate Series
#[macro_export]
macro_rules! series {
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

/// generate Dataframe
#[macro_export]
macro_rules! df {
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
