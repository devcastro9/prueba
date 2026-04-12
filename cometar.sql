CREATE OR REPLACE FUNCTION PA.F_Direccion(pCodPersona VARCHAR2) RETURN VARCHAR2
IS
   vDireccion VARCHAR2(160);
   vOrden NUMBER(1);
   --
   CURSOR curDir IS
   SELECT DECODE(es_default,
                    'S',DECODE(tip_direccion,'C',1,2),
                        DECODE(tip_direccion,'C',3,4)) orden,
          NVL(detalle,'NO REGISTRADA')
     FROM DIR_PERSONAS
    WHERE cod_persona=pCodPersona
    ORDER BY 1;
   --
BEGIN
   OPEN curDir;
   FETCH curDir INTO vOrden, vDireccion;
   CLOSE curDir;
   RETURN String_Valido(vDireccion);
END;
/