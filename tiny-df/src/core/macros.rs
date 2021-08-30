//! Tiny DF Macro

/// generate 1D vector
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

/// generate Series
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

/// generate 2D vector
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

/// generate Dataframe, default "vertical" orient data
#[macro_export]
macro_rules! df {
    [$($name:expr => [$($x:expr),* $(,)*]),+ $(,)*] => {
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
            Dataframe::from_series(buf, "v")
        }
    };
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
            Dataframe::from_series(buf, $orient)
        }
    };
    [$([$($x:expr),* $(,)*]),+ $(,)*] => {
        {
            let data = vec![
                $(
                    {
                        let mut buf_vec: Vec<DataframeData> = Vec::new();
                        $(
                            buf_vec.push($x.into());
                        )*
                        buf_vec
                    },
                )*
            ];
            Dataframe::from_vec(data, "v")
        }
    };
    [$orient:expr; $([$($x:expr),* $(,)*]),+ $(,)*] => {
        {
            let data = vec![
                $(
                    {
                        let mut buf_vec: Vec<DataframeData> = Vec::new();
                        $(
                            buf_vec.push($x.into());
                        )*
                        buf_vec
                    },
                )*
            ];
            Dataframe::from_vec(data, $orient)
        }
    };
}
