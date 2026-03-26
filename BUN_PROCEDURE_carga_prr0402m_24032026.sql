create or replace procedure pr.carga_prr0402m (
   p_cod_cliente in varchar2,
   p_fecha_hoy   in date,
   p_error       out varchar2
) is
-- mocastro - FSN-2026-03-24 - PR_ANTEC_CRED_GTT: Procedimiento wrapper para carga multi-empresa sin commits intermedios
-- Fecha de creacion : 24/03/2026
-- Objetivo          : Procedimiento wrapper que carga la GTT PR_ANTEC_CRED_GTT
--                     con informacion crediticia de un cliente para multiples empresas.
--                     Itera sobre las empresas definidas en el parametro ANTECEDENTES_CRED
--                     (PA.PARAM_GENERALES, formato '|COD1|COD2|...') y llama a
--                     carga_prr0402g con p_commit=>FALSE para cada empresa, evitando
--                     COMMITs intermedios entre empresas. Un unico COMMIT se ejecuta
--                     al finalizar todas las empresas exitosamente, centralizando el
--                     manejo transaccional de la carga multi-empresa.
--                     La GTT es ON COMMIT PRESERVE ROWS; el DELETE inicial garantiza
--                     que no queden datos de ejecuciones anteriores en la misma sesion.
--
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
         p_error,
         p_commit => FALSE
      );
      if p_error is not null then
         p_error := 'Error en carga_prr0402g para empresa '
                    || reg.cod_empresa
                    || ': '
                    || p_error;
         return;
      end if;
   end loop;
   commit;
exception
   when others then
      p_error := 'Error en carga_prr0402m: ' || sqlerrm;
      rollback;
end carga_prr0402m;
/