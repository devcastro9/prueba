-- AFTER-REPORT Trigger
DECLARE
  v_error VARCHAR2(4000);
  v_dest  VARCHAR2(200);
  v_fmt   VARCHAR2(100);
  v_mod   VARCHAR2(100);
  v_file  VARCHAR2(200);
  v_pag   NUMBER;
BEGIN
  SRW.GET_REPORT_PROPERTY(SRW.DESNAME,   v_dest);
  SRW.GET_REPORT_PROPERTY(SRW.DESFORMAT, v_fmt);
  SRW.GET_REPORT_PROPERTY(SRW.MODULE,    v_mod);
  SRW.GET_REPORT_PROPERTY(SRW.FILENAME,  v_file);
  SRW.GET_PAGE_NUM(v_pag);

  PA.PA_REGISTRA_IMPRESION(
    'REPORTS12C',
    SUBSTR(v_mod, 1, 20),
    :P_AGENCIA,
    :P_CLIENTE,
    :P_OPERACION,
    SUBSTR(v_file, 1, 100),
    v_pag,
    SUBSTR(v_dest, 1, 200),
    v_mod || '|' || v_fmt || '|' || v_dest || '|' || USER,
    v_error
  );

  IF v_error IS NOT NULL THEN
    SRW.MESSAGE(100, v_error);
  END IF;
END;