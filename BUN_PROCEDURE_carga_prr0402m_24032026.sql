create or replace procedure pr.carga_prr0402m (
   p_sesion      in varchar2,
   p_cod_cliente in varchar2,
   p_fecha_hoy   in date,
   p_error       out varchar2
) is
--
-- Procedimiento Wrapper para cargar informacion crediticia de múltiples empresas
-- Fecha de creación : 24/03/2026
-- Objetivo          : Insertar en la tabla PR_OPERACIONES_TMP los registros
--                     para múltiples códigos de empresa, manteniendo la
--                     misma sesión (p_sesion) y cliente (p_cod_cliente) para todos los registros
   v_linea          PA.PARAM_GENERALES.ABREV_PARAMETRO%TYPE;
   v_cod_moneda_usd PA.PARAM_GENERALES.ABREV_PARAMETRO%TYPE;
   v_nombre         PA.PERSONAS.NOMBRE%TYPE;
   v_ant_cred       PA.PARAM_GENERALES.ABREV_PARAMETRO%TYPE; -- cache del parametro para evitar N llamadas dentro del cursor
begin
   p_error := null;
   v_linea          := pa.PARAMETRO_GENERAL('PR', 'COD_OPER_LINEAS_CR');
   v_cod_moneda_usd := pa.PARAMETRO_GENERAL('PR', 'COD_MONEDA_DOLAR');
   v_nombre         := pa.F_RET_NOM_CLIENTE(p_cod_cliente);
   -- Se cachea el parametro una sola vez antes del loop para evitar una llamada a funcion por cada empresa
   v_ant_cred       := pa.PARAMETRO_GENERAL('PR', 'ANTECEDENTES_CRED');
   
   delete from pr.pr_operaciones_tmp where sesion = p_sesion;

   for reg in (
      select e.cod_empresa
        from pa.empresa e
       where instr(v_ant_cred, '|' || e.cod_empresa || '|') > 0
   ) loop
      pr.carga_prr0402g(
         p_sesion,
         reg.cod_empresa,
         p_cod_cliente,
         v_linea,
         v_nombre,
         v_cod_moneda_usd,
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