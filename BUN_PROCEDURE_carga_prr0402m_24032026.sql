create or replace procedure pr.carga_prr0402m (
   p_cod_cliente in varchar2,
   p_fecha_hoy   in date,
   p_error       out varchar2
) is
--
-- Procedimiento Wrapper para cargar informacion crediticia de múltiples empresas
-- Fecha de creación : 24/03/2026
-- Objetivo          : Insertar en la tabla PR_ANTEC_CRED_GTT los registros
--                     para múltiples códigos de empresa por cliente
   v_ant_cred       PA.PARAM_GENERALES.ABREV_PARAMETRO%TYPE;
BEGIN
   p_error := null;
   v_ant_cred := pa.PARAMETRO_GENERAL('PR', 'ANTECEDENTES_CRED');
   
   DELETE FROM pr.PR_ANTEC_CRED_GTT;

   for reg in (
      select e.cod_empresa
        from pa.empresa e
       where instr(v_ant_cred, '|' || e.cod_empresa || '|') > 0
   ) loop
      pr.carga_prr0402g(
         reg.cod_empresa,
         p_cod_cliente,
         p_fecha_hoy,
         p_error
      );
      if p_error is not null then
         p_error := 'Error en carga_prr0402g para empresa '
                    || reg.cod_empresa
                    || ': '
                    || p_error;
         return;
      end if;
   end loop;
exception
   when others then
      p_error := 'Error no controlado en carga_prr0402m: ' || sqlerrm;
end carga_prr0402m;
/