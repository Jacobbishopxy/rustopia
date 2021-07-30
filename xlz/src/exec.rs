use std::marker::PhantomData;
use std::mem;

use crate::core::worksheet::Cell;
use crate::Workbook;

pub trait Exec {
    type OutType;
    type ErrorType;

    fn transform(cell: Cell) -> Self::OutType;

    fn exec(batch: Vec<Vec<Self::OutType>>) -> Result<(), Self::ErrorType>;
}

pub struct Executor<E>
where
    E: Exec,
{
    wb: Workbook,
    e: PhantomData<E>,
}

impl<E> Executor<E>
where
    E: Exec,
{
    pub fn new(workbook: Workbook) -> Self {
        Self {
            wb: workbook,
            e: PhantomData,
        }
    }

    pub fn exec(&mut self, sheet: &str, batch_size: Option<usize>) -> Result<(), E::ErrorType> {
        let sheets = self.wb.sheets();
        let sheet = sheets.get(sheet).unwrap();

        let mut row_buf = Vec::new();
        let mut batch = Vec::new();

        let mut sz = 0usize;

        for row in sheet.rows(&mut self.wb) {
            for cell in row.0 {
                row_buf.push(E::transform(cell));
            }

            let mut cache_row = Vec::new();
            mem::swap(&mut cache_row, &mut row_buf);
            batch.push(cache_row);

            sz += 1;

            if let Some(bs) = batch_size {
                if sz == bs {
                    let mut cache_batch = Vec::new();
                    mem::swap(&mut cache_batch, &mut batch);
                    E::exec(cache_batch)?;
                    sz = 0;
                } else {
                    continue;
                }
            }
        }

        if batch.len() > 0 {
            let mut cache_batch = Vec::new();
            mem::swap(&mut cache_batch, &mut batch);
            E::exec(cache_batch)?;
        }

        Ok(())
    }
}

#[test]
fn test_exec() {
    use thiserror::Error;

    use crate::reader::Source;
    use crate::se::DataframeData;

    #[derive(Error, Debug)]
    enum DevError {
        #[error("unknown error")]
        Unknown,
    }

    struct T;

    impl Exec for T {
        type OutType = DataframeData;

        type ErrorType = DevError;

        fn transform(cell: Cell) -> Self::OutType {
            cell.value.into()
        }

        fn exec(batch: Vec<Vec<Self::OutType>>) -> Result<(), Self::ErrorType> {
            println!("{:?}", batch);
            println!("-----------------------------");

            Ok(())
        }
    }

    let wb = Source::Path("test.xlsx").read().unwrap();
    let mut exec = Executor::<T>::new(wb);

    if let Ok(_) = exec.exec("Dev", Some(3)) {
        //
    }
}
