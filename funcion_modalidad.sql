CREATE OR REPLACE FUNCTION pr.empresa_en_antec_cred (
    vp_cod_empresa   IN VARCHAR2)
    RETURN BOOLEAN
IS
    -- mocastro - FSN-2026-03-26 Funcion auxiliar de retrocompatibilidad para empresas no incluidas en el parametro.
    -- Fecha de creacion : 25/03/2026
    -- Objetivo          : Indica si una empresa esta contemplada en el parametro general
    --                     ANTECEDENTES_CRED (PA.PARAM_GENERALES, esquema PR,
    --                     formato '|COD1|COD2|...|').
    --                     Retorna TRUE  si vp_cod_empresa figura en el parametro.
    --                     Retorna FALSE si vp_cod_empresa es NULL o no figura en el parametro.
    --
    vr_ant_cred   PA.PARAM_GENERALES.ABREV_PARAMETRO%TYPE;
    vl_result     BOOLEAN;
BEGIN
    IF vp_cod_empresa IS NULL THEN
        vl_result := FALSE;
    ELSE
        vr_ant_cred := pa.PARAMETRO_GENERAL('PR', 'ANTECEDENTES_CRED');
        vl_result   := INSTR(vr_ant_cred, '|' || vp_cod_empresa || '|') > 0;
    END IF;

    RETURN vl_result;
END empresa_en_antec_cred;
/
