"""
def test_successful_connection_rnacentral():
    rnacentral_host = "hh-pgsql-public.ebi.ac.uk"
    rnacentral_port = "5432"
    rnacentral_dbname = "pfmegrnargs"
    rnacentral_user = "reader"
    rnacentral_password = "NWDMCE5xdipIjRrp"

    conn = get_pg_connection(
        rnacentral_host,
        rnacentral_port,
        rnacentral_dbname,
        rnacentral_user,
        rnacentral_password,
    )
    assert conn is not None, "Expected successful connection to RNAcentral"
    conn.close()
    """
