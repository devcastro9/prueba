PROCEDURE VERIFICA_ANTECRED IS
   v_param   VARCHAR2(500);
   v_en_ant  BOOLEAN;
   v_idx     NUMBER := 1;
   v_actual  VARCHAR2(20);
BEGIN
   v_en_ant := pr.empresa_en_antec_cred(:GLOBAL.CodEmpresa);

   IF v_en_ant THEN
      v_actual := :BKCONTROL.LST_EMPRESA;          -- <-- guardar selección actual
      :variables.multi_empresa := 'S';
      v_param := pa.parametro_general('PR','ANTECEDENTES_CRED');

      CLEAR_LIST('BKCONTROL.LST_EMPRESA');
      FOR reg IN (...) LOOP
         ADD_LIST_ELEMENT('BKCONTROL.LST_EMPRESA', v_idx, reg.tit_reportes, reg.cod_empresa);
         v_idx := v_idx + 1;
      END LOOP;

      :BKCONTROL.LST_EMPRESA :=
            NVL(v_actual,                          -- <-- prioridad a lo que el usuario eligió
                NVL(:VARIABLES.CodEmpresa, :GLOBAL.CodEmpresa));
      :variables.CodEmpresa := :BKCONTROL.LST_EMPRESA;   -- mantener sincronizado
      SET_ITEM_PROPERTY('BKCONTROL.LST_EMPRESA', VISIBLE, PROPERTY_TRUE);
      :variables.nom_reporte := 'PRR0402M';
   ELSE
      ...
   END IF;
END;
