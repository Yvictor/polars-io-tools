use ::polars_io_tools::mssql;
use pyo3::prelude::*;
use pyo3::types::PyCapsule;

mod example;

pub use example::Example;

#[pyfunction]
#[allow(clippy::too_many_arguments)]
#[pyo3(signature = (
    host,
    port,
    database,
    user,
    password,
    query,
    partition_on=None,
    partition_min=None,
    partition_max=None,
    partition_num=1,
    batch_size=65_536,
    channel_capacity=16,
    trust_server_certificate=false,
))]
fn mssql_native_arrow_stream(
    py: Python<'_>,
    host: String,
    port: u16,
    database: String,
    user: String,
    password: String,
    query: String,
    partition_on: Option<String>,
    partition_min: Option<i64>,
    partition_max: Option<i64>,
    partition_num: usize,
    batch_size: usize,
    channel_capacity: usize,
    trust_server_certificate: bool,
) -> PyResult<Py<PyAny>> {
    let options = mssql::MssqlReadOptions {
        host,
        port,
        database,
        user,
        password,
        trust_server_certificate,
        query,
        partition_on,
        partition_min,
        partition_max,
        partition_num,
        batch_size,
        channel_capacity,
    };
    let stream = py
        .detach(move || mssql::mssql_arrow_stream(options))
        .map_err(pyo3::exceptions::PyRuntimeError::new_err)?;
    let capsule = PyCapsule::new_with_value(py, stream, c"arrow_array_stream")?;
    Ok(capsule.into_any().unbind())
}

#[pymodule]
fn polars_io_tools(_py: Python, m: &Bound<PyModule>) -> PyResult<()> {
    // Example
    m.add_class::<Example>().unwrap();
    m.add_function(wrap_pyfunction!(mssql_native_arrow_stream, m)?)?;
    Ok(())
}
