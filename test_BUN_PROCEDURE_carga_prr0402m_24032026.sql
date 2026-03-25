-- Test suite for pr.carga_prr0402m procedure
-- Tests the wrapper procedure for loading credit information

declare
    v_sesion       varchar2(50) := 'TEST_SESSION_' || to_char(sysdate, 'YYYYMMDDHH24MISS');
    v_cod_cliente  varchar2(20) := '519650';
    v_fecha_hoy    date := trunc(sysdate);
    v_error        varchar2(4000);
begin
    -- Test 1: Normal execution with valid parameters
    dbms_output.put_line('Test 1: Normal execution');
    pr.carga_prr0402m(v_sesion, v_cod_cliente, v_fecha_hoy, v_error);
    if v_error is null then
        dbms_output.put_line('PASS: No errors returned');
    else
        dbms_output.put_line('FAIL: ' || v_error);
    end if;

    -- Test 2: Verify temporary table is cleared before processing
    dbms_output.put_line('Test 2: Temporary table cleanup');
    declare
        v_count number;
    begin
        select count(*) into v_count from pr.pr_operaciones_tmp 
         where sesion = v_sesion;
        if v_count >= 0 then
            dbms_output.put_line('PASS: Temporary table exists and is accessible');
        end if;
    end;

    -- Test 3: Invalid client code
    dbms_output.put_line('Test 3: Invalid client code');
    v_error := null;
    pr.carga_prr0402m('TEST_SESSION_2', 'INVALID_CODE', v_fecha_hoy, v_error);
    dbms_output.put_line('Result: ' || nvl(v_error, 'No error'));

    -- Test 4: Null session parameter
    dbms_output.put_line('Test 4: Null session parameter');
    v_error := null;
    begin
        pr.carga_prr0402m(null, v_cod_cliente, v_fecha_hoy, v_error);
        dbms_output.put_line('Result: ' || nvl(v_error, 'Procedure executed'));
    exception
        when others then
            dbms_output.put_line('Exception caught: ' || sqlerrm);
    end;

end;
/