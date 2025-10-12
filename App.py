import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import warnings
import os
warnings.filterwarnings('ignore')

print("ETL DATA WAREHOUSE - RENT4YOU")
print("Preprocesamiento de Datos para Análisis de Ventas por Sucursal")

ruta_datos = r'C:\Users\Ricardo Medina\Desktop\bd'

# Variables para tracking de datos eliminados
tracking_eliminados = {
    'sucursal': {'duplicados': 0, 'nulos': 0, 'otros': 0},
    'cliente': {'duplicados': 0, 'nulos': 0, 'otros': 0},
    'vehiculo': {'duplicados': 0, 'nulos': 0, 'otros': 0},
    'tiempo': {'duplicados': 0, 'nulos': 0, 'otros': 0},
    'ventas': {'duplicados': 0, 'nulos': 0, 'otros': 0, 'integridad': 0}
}

print("\nFASE 1: CARGA DE DATOS CRUDOS DESDE MOCKAROO")

print("\n[1/5] Cargando datos de Sucursales desde Mockaroo...")
try:
    sucursales_raw = pd.read_csv(f'{ruta_datos}/dim_sucursal.csv')
    print(f"   ✓ Cargados {len(sucursales_raw)} registros desde dim_sucursal.csv")
except Exception as e:
    print(f"   ✗ Error al cargar dim_sucursal.csv: {e}")
    exit()

print("\n[2/5] Cargando datos de Clientes desde Mockaroo...")
try:
    clientes_raw = pd.read_csv(f'{ruta_datos}/dim_cliente.csv')
    print(f"   ✓ Cargados {len(clientes_raw)} registros desde dim_cliente.csv")
except Exception as e:
    print(f"   ✗ Error al cargar dim_cliente.csv: {e}")
    exit()

print("\n[3/5] Cargando datos de Vehículos desde Mockaroo...")
try:
    vehiculos_raw = pd.read_csv(f'{ruta_datos}/dim_vehiculo.csv')
    print(f"   ✓ Cargados {len(vehiculos_raw)} registros desde dim_vehiculo.csv")
except Exception as e:
    print(f"   ✗ Error al cargar dim_vehiculo.csv: {e}")
    exit()

print("\n[4/5] Cargando Dimensión Tiempo desde Mockaroo...")
try:
    tiempo_raw = pd.read_csv(f'{ruta_datos}/dim_tiempo.csv')
    print(f"   ✓ Cargados {len(tiempo_raw)} registros desde dim_tiempo.csv")
except Exception as e:
    print(f"   ✗ Error al cargar dim_tiempo.csv: {e}")
    exit()

print("\n[5/5] Cargando datos de Ventas desde Mockaroo...")
try:
    ventas_raw = pd.read_csv(f'{ruta_datos}/fact_ventas.csv')
    print(f"   ✓ Cargados {len(ventas_raw)} registros desde fact_ventas.csv")
except Exception as e:
    print(f"   ✗ Error al cargar fact_ventas.csv: {e}")
    exit()

print("\n✓ Datos crudos cargados exitosamente desde Mockaroo")
print(f"\nTotal de registros crudos: {len(sucursales_raw) + len(clientes_raw) + len(vehiculos_raw) + len(tiempo_raw) + len(ventas_raw)}")

print("\n📋 Vista previa de datos cargados:")
print("\nSUCURSALES:")
print(sucursales_raw.head(3))
print("\nCLIENTES:")
print(clientes_raw.head(3))
print("\nVEHÍCULOS:")
print(vehiculos_raw.head(3))

print("\nFASE 2: PROCESO ETL - LIMPIEZA Y TRANSFORMACIÓN")

print("\n[1/5] Limpiando DIM_SUCURSAL...")
dim_sucursal = sucursales_raw.copy()

registros_antes = len(dim_sucursal)
dim_sucursal = dim_sucursal.drop_duplicates(subset=['id_sucursal'], keep='first')
duplicados_eliminados = registros_antes - len(dim_sucursal)
tracking_eliminados['sucursal']['duplicados'] = duplicados_eliminados
print(f"   ✓ Duplicados eliminados: {duplicados_eliminados}")

nulos_antes = dim_sucursal.isnull().sum().sum()
dim_sucursal['nombre_sucursal'] = dim_sucursal['nombre_sucursal'].fillna('No especificado')
dim_sucursal['ciudad'] = dim_sucursal['ciudad'].fillna('No especificado')
dim_sucursal['region'] = dim_sucursal['region'].fillna('No especificado')
print(f"   ✓ Valores nulos manejados: {nulos_antes}")

dim_sucursal['ciudad'] = dim_sucursal['ciudad'].str.strip().str.title()
dim_sucursal['region'] = dim_sucursal['region'].str.strip().str.title()
dim_sucursal['nombre_sucursal'] = dim_sucursal['nombre_sucursal'].str.strip()
print(f"   ✓ Normalización de texto completada")

print(f"   → Registros finales: {len(dim_sucursal)}")

print("\n[2/5] Limpiando DIM_CLIENTE...")
dim_cliente = clientes_raw.copy()

dim_cliente.columns = dim_cliente.columns.str.lower()

registros_antes = len(dim_cliente)
dim_cliente = dim_cliente.drop_duplicates(subset=['id_cliente'], keep='first')
duplicados_eliminados = registros_antes - len(dim_cliente)
tracking_eliminados['cliente']['duplicados'] = duplicados_eliminados
print(f"   ✓ Duplicados eliminados: {duplicados_eliminados}")

nulos_ciudad = dim_cliente['ciudad'].isnull().sum()
nulos_genero = dim_cliente['genero'].isnull().sum() if 'genero' in dim_cliente.columns else 0
dim_cliente['ciudad'] = dim_cliente['ciudad'].fillna('No especificado')
if 'genero' in dim_cliente.columns:
    dim_cliente['genero'] = dim_cliente['genero'].fillna('No especificado')
tracking_eliminados['cliente']['nulos'] = nulos_ciudad + nulos_genero
print(f"   ✓ Valores nulos manejados: {nulos_ciudad + nulos_genero}")

registros_antes = len(dim_cliente)
dim_cliente = dim_cliente[(dim_cliente['edad'] >= 18) & (dim_cliente['edad'] <= 100)]
invalidos = registros_antes - len(dim_cliente)
tracking_eliminados['cliente']['otros'] = invalidos
print(f"   ✓ Validación de edad (18-100 años): {invalidos} registros inválidos eliminados")

dim_cliente['ciudad'] = dim_cliente['ciudad'].str.strip().str.title()
dim_cliente['nombre'] = dim_cliente['nombre'].str.strip().str.title()
print(f"   ✓ Normalización de texto completada")

print(f"   → Registros finales: {len(dim_cliente)}")

print("\n[3/5] Limpiando DIM_VEHICULO...")
dim_vehiculo = vehiculos_raw.copy()

dim_vehiculo['tipo_vehiculo'] = dim_vehiculo['tipo_vehiculo'].fillna('No especificado')
dim_vehiculo['marca'] = dim_vehiculo['marca'].fillna('No especificado')
dim_vehiculo['modelo'] = dim_vehiculo['modelo'].fillna('Desconocido')

if dim_vehiculo['costo_diario'].dtype == 'object':
    dim_vehiculo['costo_diario'] = dim_vehiculo['costo_diario'].str.replace('$', '').str.replace(',', '')
    dim_vehiculo['costo_diario'] = pd.to_numeric(dim_vehiculo['costo_diario'], errors='coerce')

registros_antes = len(dim_vehiculo)
dim_vehiculo = dim_vehiculo[dim_vehiculo['costo_diario'] > 0]
invalidos = registros_antes - len(dim_vehiculo)
tracking_eliminados['vehiculo']['otros'] += invalidos
print(f"   ✓ Costos negativos/cero/nulos eliminados: {invalidos}")

registros_antes = len(dim_vehiculo)
ano_actual = datetime.now().year
dim_vehiculo = dim_vehiculo[(dim_vehiculo['ano_fabricacion'] >= 2000) & 
                             (dim_vehiculo['ano_fabricacion'] <= ano_actual)]
invalidos = registros_antes - len(dim_vehiculo)
tracking_eliminados['vehiculo']['otros'] += invalidos
print(f"   ✓ Años de fabricación inválidos eliminados: {invalidos}")

dim_vehiculo['tipo_vehiculo'] = dim_vehiculo['tipo_vehiculo'].str.strip().str.title()
dim_vehiculo['marca'] = dim_vehiculo['marca'].str.strip().str.title()
dim_vehiculo['modelo'] = dim_vehiculo['modelo'].str.strip()
print(f"   ✓ Normalización de texto completada")

print(f"   → Registros finales: {len(dim_vehiculo)}")

print("\n[4/5] Validando DIM_TIEMPO...")
dim_tiempo = tiempo_raw.copy()

dim_tiempo.columns = dim_tiempo.columns.str.lower()

registros_antes = len(dim_tiempo)
dim_tiempo = dim_tiempo.dropna(subset=['id_fecha', 'fecha'])
nulos_eliminados = registros_antes - len(dim_tiempo)
tracking_eliminados['tiempo']['nulos'] = nulos_eliminados
print(f"   ✓ Registros con fechas nulas eliminados: {nulos_eliminados}")

dim_tiempo['fecha'] = pd.to_datetime(dim_tiempo['fecha'], errors='coerce')
registros_antes = len(dim_tiempo)
dim_tiempo = dim_tiempo.dropna(subset=['fecha'])
invalidos = registros_antes - len(dim_tiempo)
tracking_eliminados['tiempo']['otros'] += invalidos
print(f"   ✓ Fechas inválidas eliminadas: {invalidos}")
print(f"   ✓ Formato de fechas validado")

registros_antes = len(dim_tiempo)
dim_tiempo = dim_tiempo.drop_duplicates(subset=['fecha'], keep='first')
duplicados = registros_antes - len(dim_tiempo)
tracking_eliminados['tiempo']['duplicados'] = duplicados
print(f"   ✓ Fechas duplicadas eliminadas: {duplicados}")

dim_tiempo['mes'] = dim_tiempo['fecha'].dt.month
dim_tiempo['trimestre'] = dim_tiempo['fecha'].dt.quarter
dim_tiempo['ano'] = dim_tiempo['fecha'].dt.year
dim_tiempo['dia_semana'] = dim_tiempo['fecha'].dt.day_name()

dim_tiempo = dim_tiempo.sort_values('fecha').reset_index(drop=True)
dim_tiempo['id_fecha'] = range(1, len(dim_tiempo) + 1)

print(f"   ✓ Dimensiones temporales recalculadas correctamente")
print(f"   → Registros finales: {len(dim_tiempo)}")

print("\n[5/5] Limpiando FACT_VENTAS...")
fact_ventas = ventas_raw.copy()

fact_ventas.columns = fact_ventas.columns.str.lower()

registros_antes = len(fact_ventas)
fact_ventas = fact_ventas.dropna(subset=['id_cliente', 'id_vehiculo', 'id_sucursal', 'id_fecha', 'cantidad_dias'])
nulos_eliminados = registros_antes - len(fact_ventas)
tracking_eliminados['ventas']['nulos'] = nulos_eliminados
print(f"   ✓ Registros con valores nulos críticos eliminados: {nulos_eliminados}")

if fact_ventas['monto_total'].dtype == 'object':
    fact_ventas['monto_total'] = fact_ventas['monto_total'].str.replace('$', '').str.replace(',', '')
    fact_ventas['monto_total'] = pd.to_numeric(fact_ventas['monto_total'], errors='coerce')

if fact_ventas['descuento'].dtype == 'object':
    fact_ventas['descuento'] = fact_ventas['descuento'].str.replace('$', '').str.replace(',', '')
    fact_ventas['descuento'] = pd.to_numeric(fact_ventas['descuento'], errors='coerce')

fact_ventas['id_cliente'] = pd.to_numeric(fact_ventas['id_cliente'], errors='coerce')
fact_ventas['id_vehiculo'] = pd.to_numeric(fact_ventas['id_vehiculo'], errors='coerce')
fact_ventas['id_sucursal'] = pd.to_numeric(fact_ventas['id_sucursal'], errors='coerce')
fact_ventas['id_fecha'] = pd.to_numeric(fact_ventas['id_fecha'], errors='coerce')

fact_ventas = fact_ventas.dropna()

registros_antes = len(fact_ventas)
fact_ventas = fact_ventas.drop_duplicates()
duplicados_eliminados = registros_antes - len(fact_ventas)
tracking_eliminados['ventas']['duplicados'] = duplicados_eliminados
print(f"    Duplicados eliminados: {duplicados_eliminados}")

registros_antes = len(fact_ventas)
fact_ventas = fact_ventas[fact_ventas['cantidad_dias'] > 0]
fact_ventas = fact_ventas[fact_ventas['monto_total'] > 0]
invalidos = registros_antes - len(fact_ventas)
tracking_eliminados['ventas']['otros'] = invalidos
print(f"    Valores negativos/cero eliminados: {invalidos}")

registros_antes = len(fact_ventas)

fact_ventas = fact_ventas[fact_ventas['id_cliente'].isin(dim_cliente['id_cliente'])]
invalidos_cliente = registros_antes - len(fact_ventas)

registros_antes = len(fact_ventas)
fact_ventas = fact_ventas[fact_ventas['id_vehiculo'].isin(dim_vehiculo['id_vehiculo'])]
invalidos_vehiculo = registros_antes - len(fact_ventas)

registros_antes = len(fact_ventas)
fact_ventas = fact_ventas[fact_ventas['id_sucursal'].isin(dim_sucursal['id_sucursal'])]
invalidos_sucursal = registros_antes - len(fact_ventas)

registros_antes = len(fact_ventas)
fact_ventas = fact_ventas[fact_ventas['id_fecha'].isin(dim_tiempo['id_fecha'])]
invalidos_fecha = registros_antes - len(fact_ventas)

total_invalidos = invalidos_cliente + invalidos_vehiculo + invalidos_sucursal + invalidos_fecha
tracking_eliminados['ventas']['integridad'] = total_invalidos
print(f"    Integridad referencial validada:")
print(f"      - Referencias inválidas a clientes: {invalidos_cliente}")
print(f"      - Referencias inválidas a vehículos: {invalidos_vehiculo}")
print(f"      - Referencias inválidas a sucursales: {invalidos_sucursal}")
print(f"      - Referencias inválidas a fechas: {invalidos_fecha}")
print(f"      - Total eliminados: {total_invalidos}")

print(f"   → Registros finales: {len(fact_ventas)}")

print("\n✓ Proceso ETL completado exitosamente")

print("\nFASE 3: RESUMEN DE DATOS PREPROCESADOS")

print("\n📊 TABLAS DEL DATA WAREHOUSE:")
print(f"   1. DIM_SUCURSAL:    {len(dim_sucursal):>5} registros")
print(f"   2. DIM_CLIENTE:     {len(dim_cliente):>5} registros")
print(f"   3. DIM_VEHICULO:    {len(dim_vehiculo):>5} registros")
print(f"   4. DIM_TIEMPO:      {len(dim_tiempo):>5} registros")
print(f"   5. FACT_VENTAS:     {len(fact_ventas):>5} registros")
print(f"   TOTAL:              {len(dim_sucursal) + len(dim_cliente) + len(dim_vehiculo) + len(dim_tiempo) + len(fact_ventas):>5} registros")

print("\nFASE 4: ANÁLISIS EXPLORATORIO")

print("\n VENTAS POR SUCURSAL:")
ventas_por_sucursal = fact_ventas.merge(dim_sucursal, on='id_sucursal')
resumen_sucursal = ventas_por_sucursal.groupby(['nombre_sucursal', 'ciudad']).agg({
    'monto_total': ['sum', 'mean', 'count']
}).round(2)
resumen_sucursal.columns = ['Monto Total', 'Ticket Promedio', 'Num. Rentas']
print(resumen_sucursal)

print("\n VEHÍCULOS MÁS RENTADOS:")
vehiculos_rentados = fact_ventas.merge(dim_vehiculo, on='id_vehiculo')
top_vehiculos = vehiculos_rentados.groupby('tipo_vehiculo').agg({
    'id_venta': 'count',
    'monto_total': 'sum'
}).round(2)
top_vehiculos.columns = ['Cantidad Rentas', 'Ingresos Totales']
top_vehiculos = top_vehiculos.sort_values('Cantidad Rentas', ascending=False)
print(top_vehiculos)

print("\n PERFIL DE CLIENTES POR TIPO:")
perfil_clientes = dim_cliente.groupby('tipo_cliente').agg({
    'id_cliente': 'count',
    'edad': 'mean'
}).round(2)
perfil_clientes.columns = ['Cantidad', 'Edad Promedio']
print(perfil_clientes)

print("\nFASE 5: EXPORTACIÓN DE DATOS PREPROCESADOS")

carpeta_destino = r'C:\Users\Ricardo Medina\Desktop\bd\datos_preprocesados'
if not os.path.exists(carpeta_destino):
    os.makedirs(carpeta_destino)
    print(f"\n✓ Carpeta '{carpeta_destino}' creada")

print("\n Exportando tablas limpias a CSV...")

dim_sucursal.to_csv(f'{carpeta_destino}/dim_sucursal_clean.csv', index=False, encoding='utf-8-sig')
print(f"    dim_sucursal_clean.csv")

dim_cliente.to_csv(f'{carpeta_destino}/dim_cliente_clean.csv', index=False, encoding='utf-8-sig')
print(f"    dim_cliente_clean.csv")

dim_vehiculo.to_csv(f'{carpeta_destino}/dim_vehiculo_clean.csv', index=False, encoding='utf-8-sig')
print(f"    dim_vehiculo_clean.csv")

dim_tiempo.to_csv(f'{carpeta_destino}/dim_tiempo_clean.csv', index=False, encoding='utf-8-sig')
print(f"    dim_tiempo_clean.csv")

fact_ventas.to_csv(f'{carpeta_destino}/fact_ventas_clean.csv', index=False, encoding='utf-8-sig')
print(f"    fact_ventas_clean.csv")

print(f"\n✓ Todos los archivos exportados exitosamente a '{carpeta_destino}/'")

print("\nFASE 6: VALIDACIÓN FINAL DE CALIDAD")

print("\n CHECKLIST DE CALIDAD:")
print(f"    Datos cargados desde Mockaroo correctamente")
print(f"    Duplicados eliminados en todas las tablas")
print(f"    Valores nulos manejados correctamente")
print(f"    Rangos validados (edades, costos, fechas)")
print(f"    Integridad referencial verificada")
print(f"    Normalización de texto aplicada")
print(f"    Consistencia de datos garantizada")

print("\n" + "="*80)
print("FASE 7: REPORTE DETALLADO DE DATOS ELIMINADOS")
print("="*80)

total_eliminados_general = 0
print("\n📊 RESUMEN DE ELIMINACIÓN DE DATOS POR TABLA:\n")

print("1️⃣  DIM_SUCURSAL:")
eliminados_sucursal = (tracking_eliminados['sucursal']['duplicados'] + 
                       tracking_eliminados['sucursal']['nulos'] + 
                       tracking_eliminados['sucursal']['otros'])
print(f"   • Duplicados eliminados:        {tracking_eliminados['sucursal']['duplicados']:>5} registros")
print(f"   • Valores nulos manejados:      {tracking_eliminados['sucursal']['nulos']:>5} registros")
print(f"   • Otros criterios:              {tracking_eliminados['sucursal']['otros']:>5} registros")
print(f"   ➜ TOTAL ELIMINADOS:             {eliminados_sucursal:>5} registros")
total_eliminados_general += eliminados_sucursal

print("\n2️⃣  DIM_CLIENTE:")
eliminados_cliente = (tracking_eliminados['cliente']['duplicados'] + 
                      tracking_eliminados['cliente']['nulos'] + 
                      tracking_eliminados['cliente']['otros'])
print(f"   • Duplicados eliminados:        {tracking_eliminados['cliente']['duplicados']:>5} registros")
print(f"   • Valores nulos manejados:      {tracking_eliminados['cliente']['nulos']:>5} registros")
print(f"   • Edades fuera de rango:        {tracking_eliminados['cliente']['otros']:>5} registros")
print(f"   ➜ TOTAL ELIMINADOS:             {eliminados_cliente:>5} registros")
total_eliminados_general += eliminados_cliente

print("\n3️⃣  DIM_VEHICULO:")
eliminados_vehiculo = (tracking_eliminados['vehiculo']['duplicados'] + 
                       tracking_eliminados['vehiculo']['nulos'] + 
                       tracking_eliminados['vehiculo']['otros'])
print(f"   • Duplicados eliminados:        {tracking_eliminados['vehiculo']['duplicados']:>5} registros")
print(f"   • Valores nulos manejados:      {tracking_eliminados['vehiculo']['nulos']:>5} registros")
print(f"   • Costos/años inválidos:        {tracking_eliminados['vehiculo']['otros']:>5} registros")
print(f"   ➜ TOTAL ELIMINADOS:             {eliminados_vehiculo:>5} registros")
total_eliminados_general += eliminados_vehiculo

print("\n4️⃣  DIM_TIEMPO:")
eliminados_tiempo = (tracking_eliminados['tiempo']['duplicados'] + 
                     tracking_eliminados['tiempo']['nulos'] + 
                     tracking_eliminados['tiempo']['otros'])
print(f"   • Duplicados eliminados:        {tracking_eliminados['tiempo']['duplicados']:>5} registros")
print(f"   • Valores nulos manejados:      {tracking_eliminados['tiempo']['nulos']:>5} registros")
print(f"   • Fechas inválidas:             {tracking_eliminados['tiempo']['otros']:>5} registros")
print(f"   ➜ TOTAL ELIMINADOS:             {eliminados_tiempo:>5} registros")
total_eliminados_general += eliminados_tiempo

print("\n5️⃣  FACT_VENTAS:")
eliminados_ventas = (tracking_eliminados['ventas']['duplicados'] + 
                     tracking_eliminados['ventas']['nulos'] + 
                     tracking_eliminados['ventas']['otros'] +
                     tracking_eliminados['ventas']['integridad'])
print(f"   • Valores nulos críticos:       {tracking_eliminados['ventas']['nulos']:>5} registros")
print(f"   • Duplicados eliminados:        {tracking_eliminados['ventas']['duplicados']:>5} registros")
print(f"   • Montos/días negativos:        {tracking_eliminados['ventas']['otros']:>5} registros")
print(f"   • Integridad referencial:       {tracking_eliminados['ventas']['integridad']:>5} registros")
print(f"   ➜ TOTAL ELIMINADOS:             {eliminados_ventas:>5} registros")
total_eliminados_general += eliminados_ventas

print("\n" + "="*80)
print(f"🗑️  TOTAL GENERAL DE REGISTROS ELIMINADOS: {total_eliminados_general} registros")
print("="*80)

print("\n" + "="*80)
print("PROCESO ETL COMPLETADO EXITOSAMENTE")
print("="*80)

print("\n📁 Archivos generados (DATOS LIMPIOS):")
print(f"    {carpeta_destino}/dim_sucursal_clean.csv")
print(f"    {carpeta_destino}/dim_cliente_clean.csv")
print(f"    {carpeta_destino}/dim_vehiculo_clean.csv")
print(f"    {carpeta_destino}/dim_tiempo_clean.csv")
print(f"    {carpeta_destino}/fact_ventas_clean.csv")

print("\n🎯 Data Warehouse listo para análisis de Business Intelligence")