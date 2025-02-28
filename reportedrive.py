import pandas as pd
import gspread
from oauth2client.service_account import ServiceAccountCredentials
import streamlit as st
from io import BytesIO, StringIO
import OTMrunReport as rr
import requests
from datetime import datetime, date, timedelta
import numpy as np

# Conectar a Google Sheets
def authenticate_gsheet(json_file, spreadsheet_name):
    scope = ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]
    creds = ServiceAccountCredentials.from_json_keyfile_name(json_file, scope)
    client = gspread.authorize(creds)
    sheet = client.open(spreadsheet_name).sheet1  # Acceder a la primera hoja
    return sheet

# Subir DataFrame a Google Sheets
def upload_to_gsheet_optimized(sheet, dataframe):
    # Limpiar la hoja de cálculo
    sheet.clear()

    # Reemplazar valores no válidos en el DataFrame
    dataframe = dataframe.replace([np.inf, -np.inf], np.nan)  # Reemplaza infinitos por NaN
    dataframe = dataframe.fillna("")  # Reemplaza NaN por cadenas vacías

    # Convertir el DataFrame a una lista de listas (incluyendo encabezados)
    data = [dataframe.columns.tolist()] + dataframe.values.tolist()

    # Escribir todos los datos en una sola operación
    sheet.update("A1", data)  # Escribe desde la celda A1

    st.success("¡Hoja limpiada y datos subidos correctamente a Google Sheets!")

def actualizacion_fecha(sheet):
    # Obtener la fecha y hora actuales
    now = datetime.now()
    fecha_actual = now.strftime("%d/%m/%Y %H:%M:%S")

    # Actualizar la celda A2 con la fecha y hora actuales
    sheet.update("A2", [[fecha_actual]])

    st.success("¡Fecha y hora de actualización agregadas correctamente!")
# Función para obtener filas con valores problemáticos
def get_invalid_rows(dataframe):
    # Filtrar filas con valores problemáticos (NaN, Infinity, -Infinity)
    invalid_rows = dataframe[
        dataframe.isin([np.inf, -np.inf]).any(axis=1) | dataframe.isnull().any(axis=1)
    ]
    return invalid_rows

# -------------------- Interfaz en Streamlit --------------------

# Título de la app
st.title("Subir DataFrame a Google Sheets")

# Cargar credenciales desde Streamlit Secrets
service_account_info = json.loads(st.secrets["GOOGLE_CREDENTIALS"])
credentials = Credentials.from_service_account_info(service_account_info)

# Autenticar con Google Sheets
client = gspread.authorize(credentials)
spreadsheet_name = "base real 2025"

provisiones = 'https://docs.google.com/spreadsheets/d/1NjwDNiOgdiRXRIe0C0ksfS0t2ImJW9yC/export?format=xlsx'
mapeo = 'https://docs.google.com/spreadsheets/d/1takBliEu8CodgMM-znG-TH023pETDnRvlfznq-g7laM/export?format=xlsx'
base = 'https://docs.google.com/spreadsheets/d/1Ny7GxH1ls6ax2FZqwfdVH-T530vWC7cdOipDrZkyn38/export?format=xlsx'
    # Función para descargar datos con cacheo
@st.cache_data
def cargar_datos(url):
        response = requests.get(url)
        response.raise_for_status()  # Verifica si hubo algún error en la descarga
        archivo_excel = BytesIO(response.content)
        return pd.read_excel(archivo_excel, engine="openpyxl")
@st.cache_data
def cargar_datos_pro(url, sheet_name=None):
    response = requests.get(url)
    response.raise_for_status()  # Verifica si hubo algún error en la descarga
    archivo_excel = BytesIO(response.content)
    return pd.read_excel(archivo_excel, sheet_name=sheet_name, engine="openpyxl")

# Especifica la hoja que deseas cargar
hoja_deseada = "Base provisiones"

df_provisiones = cargar_datos_pro(provisiones, sheet_name=hoja_deseada)
    # Descargar las hojas de cálculo

df_mapeo = cargar_datos(mapeo)
df_base = cargar_datos(base)
orden_meses = {
    1: 'ene.', 2: 'feb.', 3: 'mar.', 4: 'abr.',
    5: 'may.', 6: 'jun.', 7: 'jul.', 8: 'ago.',
    9: 'sep.', 10: 'oct.', 11: 'nov.', 12: 'dic.'
}

def contar_jueves(mes, año):
    # Obtener el primer día del mes
    primer_dia = date(año, mes, 1)
    # Calcular el día de la semana del primer día (0=Lunes, 3=Jueves)
    primer_jueves = primer_dia + timedelta(days=(3 - primer_dia.weekday()) % 7)
    
    # Contar los jueves en el mes
    jueves_count = 0
    dia_actual = primer_jueves
    while dia_actual.month == mes:
        jueves_count += 1
        dia_actual += timedelta(weeks=1)
    
    return jueves_count
# Función para procesar el archivo XTR y devolverlo como DataFrame
@st.cache_data
def get_xtr_as_dataframe():
    # 1. Obtener el reporte (contenido del archivo XTR)
    headers = rr.headers('rolmedo', 'Mexico.2022')
    algo = rr.runReport('/Custom/ESGARI/Qlik/reportesNecesarios/XXRO_EXTRACTOR_GL_REP.xdo', 'ekck.fa.us6', headers)

    # 2. Verificar el tipo de "algo"
    if isinstance(algo, bytes):
        algo = algo.decode('utf-8')  # Convertir bytes a string

    # 3. Convertir el contenido XTR a DataFrame
    try:
        xtr_io = StringIO(algo)  # Crear un buffer en memoria
        df = pd.read_csv(xtr_io, sep=",", low_memory=False)  # Ajusta el delimitador aquí
    except Exception as e:
        st.error(f"Error al procesar el archivo XTR: {e}")
        return None

    return df, algo

    # Procesar el archivo XTR y convertirlo a DataFrame
df, algo = get_xtr_as_dataframe()
df_original = df.copy()

    # Selección y renombrado de columnas
columnas_d = ['DEFAULT_EFFECTIVE_DATE', 'DEFAULT_EFFECTIVE_DATE', 'SEGMENT1', 'SEGMENT2', 'SEGMENT3', 'SEGMENT5', 'CREDIT', 'DEBIT']
nuevo_nombre = ['Año_A','Mes_A', 'Empresa_A', 'CeCo_A', 'Proyecto_A', 'Cuenta_A', 'Credit_A', 'Debit_A']

    # Validar que las columnas existen en el DataFrame
columnas_disponibles = [col for col in columnas_d if col in df.columns]


    # Seleccionar y renombrar las columnas
df = df[columnas_disponibles]
df.columns = nuevo_nombre[:len(columnas_disponibles)]  # Renombrar las columnas disponibles

df = df[df['Cuenta_A'] >= 400000000]
    # Asegurarse de que las columnas sean numéricas
df['Cuenta_A'] = pd.to_numeric(df['Cuenta_A'], errors='coerce')
df['Debit_A'] = pd.to_numeric(df['Debit_A'], errors='coerce')
df['Credit_A'] = pd.to_numeric(df['Credit_A'], errors='coerce')

    # Rellenar valores NaN con 0 (opcional, dependiendo de tus datos)
df[['Debit_A', 'Credit_A']] = df[['Debit_A', 'Credit_A']].fillna(0)

    # Calcular la columna Neto_A
df['Neto_A'] = df.apply(
        lambda row:  row['Credit_A'] - row['Debit_A'] if row['Cuenta_A'] < 500000000 else row['Debit_A'] - row['Credit_A'] ,
        axis=1
    )
df['Año_A'] = pd.to_datetime(df['Año_A'], errors='coerce')
df['Año_A'] = df['Año_A'].dt.year
años_archivo = df['Año_A'].unique().tolist()
    # Convertir la columna 'Mes_A' al tipo datetime
df['Mes_A'] = pd.to_datetime(df['Mes_A'], errors='coerce')

    # Crear una nueva columna con el mes (en formato numérico o nombre, según prefieras)
df['Mes_A'] = df['Mes_A'].dt.month  # Esto crea una columna con el número del mes


current_month = datetime.now().month
current_year = datetime.now().year
# Crear el componente multiselect
# Get the unique months from the DataFrame
meses_unicos = sorted(df['Mes_A'].dropna().unique())

# Ensure the default value is valid
if current_month in meses_unicos:
    default_value = [current_month]
else:
    default_value = []  # Empty default if the current month is not in the dataset

# Create the multiselect widget
mes_seleccionado = st.multiselect(
    "Selecciona los meses:",
    options=meses_unicos,
    default=default_value
)
año_seleccionado = st.selectbox(
     "seleccione el año",
     options=sorted(df['Año_A'].unique()),
     index=sorted(df['Año_A'].unique()).index(current_year) if current_year in df['Año_A'].unique() else 0
    )


    # Filtrar el DataFrame por el mes seleccionado
df_filtrado = df[df['Mes_A'].isin(mes_seleccionado)]
df_filtrado = df_filtrado[df_filtrado['Año_A'] == año_seleccionado]

df_filtrado['Mes_A'] = df_filtrado['Mes_A'].map(orden_meses)
df_filtrado = df_filtrado.merge(df_mapeo, on='Cuenta_A', how='left')



df_filtrado['Importe_PPTO_A'] = 0
df_filtrado['Usuario_A'] = 'Sistema'
df_filtrado['ID_A'] = range(1, len(df_filtrado) + 1)

columnas_ordenadas = ['ID_A', 'Mes_A', 'Empresa_A', 'CeCo_A', 'Proyecto_A', 'Cuenta_A', 'Clasificacion_A', 'Cuenta_Nombre_A', 'Importe_PPTO_A',
                         'Debit_A', 'Credit_A', 'Neto_A', 'Categoria_A','Usuario_A']
df_filtrado = df_filtrado[columnas_ordenadas]

if len(mes_seleccionado) == 1:

    if len(mes_seleccionado) == 1:
        mes_actual = mes_seleccionado[0]
        df_provisiones['Mes_A'] = mes_actual
        df_provisiones['Mes_A'] = df_provisiones['Mes_A'].map(orden_meses)

    opcion = st.selectbox(
        "Selecciona si deseas incluir provisiones en el DataFrame:",
        ["No incluir provisiones", "Incluir provisiones", "Incluir provisiones cierre de mes(ingresos gaby)", "Incluir provisiones cierre de mes (ingresos nosotros)"]
    )



    # Procesar según la selección
    if opcion == "Incluir provisiones":
        st.write("Las provisiones serán incluidas en el DataFrame.")

        df_filtrado = df_filtrado[~(df_filtrado['Categoria_A'] == 'INGRESO')]
        df_filtrado = df_filtrado[~((df_filtrado['Proyecto_A'] == 1001) & (df_filtrado['Categoria_A'] == 'CASETAS'))]
        df_filtrado = df_filtrado[~((df_filtrado['Proyecto_A'] == 1003) & (df_filtrado['Categoria_A'] == 'CASETAS'))]
        df_filtrado = df_filtrado[~(df_filtrado['Categoria_A'] == 'FLETES')]
        df_filtrado = df_filtrado[~(df_filtrado['CeCo_A'] == 50)]



        df_filtrado = df_filtrado[~((df_filtrado['Proyecto_A'] == 1001) & (df_filtrado['Cuenta_A'] == 510100001))]
        df_filtrado = df_filtrado[~((df_filtrado['Proyecto_A'] == 1001) & (df_filtrado['Cuenta_A'] == 510100003))]
        df_filtrado = df_filtrado[~((df_filtrado['Proyecto_A'] == 1001) & (df_filtrado['Cuenta_A'] == 510100004))]
        df_filtrado = df_filtrado[~((df_filtrado['Proyecto_A'] == 1001) & (df_filtrado['Cuenta_A'] == 510100005))]
        df_filtrado = df_filtrado[~((df_filtrado['Proyecto_A'] == 1001) & (df_filtrado['Cuenta_A'] == 510100010))]
        df_filtrado = df_filtrado[~((df_filtrado['Proyecto_A'] == 1001) & (df_filtrado['Cuenta_A'] == 510100018))]
        df_filtrado = df_filtrado[~((df_filtrado['Proyecto_A'] == 1001) & (df_filtrado['Cuenta_A'] == 510100023))]
        df_filtrado = df_filtrado[~((df_filtrado['Proyecto_A'] == 1001) & (df_filtrado['Cuenta_A'] == 510100040))]
        df_filtrado = df_filtrado[~((df_filtrado['Proyecto_A'] == 1001) & (df_filtrado['Cuenta_A'] == 510100057))]
        df_filtrado = df_filtrado[~((df_filtrado['Proyecto_A'] == 1001) & (df_filtrado['Cuenta_A'] == 510100058))]
        df_filtrado = df_filtrado[~((df_filtrado['Proyecto_A'] == 1001) & (df_filtrado['Cuenta_A'] == 510100012))]
        df_filtrado = df_filtrado[~((df_filtrado['Proyecto_A'] == 1001) & (df_filtrado['Cuenta_A'] == 510100011))]
        df_filtrado = df_filtrado[~((df_filtrado['Proyecto_A'] == 1001) & (df_filtrado['Cuenta_A'] == 510100002))]
        df_filtrado = df_filtrado[~((df_filtrado['Proyecto_A'] == 1001) & (df_filtrado['Cuenta_A'] == 511086000))]
        df_filtrado = df_filtrado[~((df_filtrado['Proyecto_A'] == 1001) & (df_filtrado['Cuenta_A'] == 510100070))]


        df_filtrado = df_filtrado[~((df_filtrado['Proyecto_A'] == 1003) & (df_filtrado['Cuenta_A'] == 510100001))]
        df_filtrado = df_filtrado[~((df_filtrado['Proyecto_A'] == 1003) & (df_filtrado['Cuenta_A'] == 510100003))]
        df_filtrado = df_filtrado[~((df_filtrado['Proyecto_A'] == 1003) & (df_filtrado['Cuenta_A'] == 510100004))]
        df_filtrado = df_filtrado[~((df_filtrado['Proyecto_A'] == 1003) & (df_filtrado['Cuenta_A'] == 510100005))]
        df_filtrado = df_filtrado[~((df_filtrado['Proyecto_A'] == 1003) & (df_filtrado['Cuenta_A'] == 510100010))]
        df_filtrado = df_filtrado[~((df_filtrado['Proyecto_A'] == 1003) & (df_filtrado['Cuenta_A'] == 510100018))]
        df_filtrado = df_filtrado[~((df_filtrado['Proyecto_A'] == 1003) & (df_filtrado['Cuenta_A'] == 510100023))]
        df_filtrado = df_filtrado[~((df_filtrado['Proyecto_A'] == 1003) & (df_filtrado['Cuenta_A'] == 510100040))]
        df_filtrado = df_filtrado[~((df_filtrado['Proyecto_A'] == 1003) & (df_filtrado['Cuenta_A'] == 510100057))]
        df_filtrado = df_filtrado[~((df_filtrado['Proyecto_A'] == 1003) & (df_filtrado['Cuenta_A'] == 510100058))]
        df_filtrado = df_filtrado[~((df_filtrado['Proyecto_A'] == 1003) & (df_filtrado['Cuenta_A'] == 510100012))]
        df_filtrado = df_filtrado[~((df_filtrado['Proyecto_A'] == 1003) & (df_filtrado['Cuenta_A'] == 510100011))]
        df_filtrado = df_filtrado[~((df_filtrado['Proyecto_A'] == 1003) & (df_filtrado['Cuenta_A'] == 510100002))]
        df_filtrado = df_filtrado[~((df_filtrado['Proyecto_A'] == 1003) & (df_filtrado['Cuenta_A'] == 510100070))]
        df_filtrado = df_filtrado[~((df_filtrado['Proyecto_A'] == 1003) & (df_filtrado['Cuenta_A'] == 511086000))]


        df_filtrado = df_filtrado[~((df_filtrado['Proyecto_A'] == 5001) & (df_filtrado['Cuenta_A'] == 510100001))]
        df_filtrado = df_filtrado[~((df_filtrado['Proyecto_A'] == 5001) & (df_filtrado['Cuenta_A'] == 510100003))]
        df_filtrado = df_filtrado[~((df_filtrado['Proyecto_A'] == 5001) & (df_filtrado['Cuenta_A'] == 510100004))]
        df_filtrado = df_filtrado[~((df_filtrado['Proyecto_A'] == 5001) & (df_filtrado['Cuenta_A'] == 510100005))]
        df_filtrado = df_filtrado[~((df_filtrado['Proyecto_A'] == 5001) & (df_filtrado['Cuenta_A'] == 510100010))]
        df_filtrado = df_filtrado[~((df_filtrado['Proyecto_A'] == 5001) & (df_filtrado['Cuenta_A'] == 510100018))]
        df_filtrado = df_filtrado[~((df_filtrado['Proyecto_A'] == 5001) & (df_filtrado['Cuenta_A'] == 510100023))]
        df_filtrado = df_filtrado[~((df_filtrado['Proyecto_A'] == 5001) & (df_filtrado['Cuenta_A'] == 510100040))]
        df_filtrado = df_filtrado[~((df_filtrado['Proyecto_A'] == 5001) & (df_filtrado['Cuenta_A'] == 510100057))]
        df_filtrado = df_filtrado[~((df_filtrado['Proyecto_A'] == 5001) & (df_filtrado['Cuenta_A'] == 510100058))]
        df_filtrado = df_filtrado[~((df_filtrado['Proyecto_A'] == 5001) & (df_filtrado['Cuenta_A'] == 510100012))]
        df_filtrado = df_filtrado[~((df_filtrado['Proyecto_A'] == 5001) & (df_filtrado['Cuenta_A'] == 510100011))]
        df_filtrado = df_filtrado[~((df_filtrado['Proyecto_A'] == 5001) & (df_filtrado['Cuenta_A'] == 510100002))]
        df_filtrado = df_filtrado[~((df_filtrado['Proyecto_A'] == 5001) & (df_filtrado['Cuenta_A'] == 510100070))]
        df_filtrado = df_filtrado[~((df_filtrado['Proyecto_A'] == 5001) & (df_filtrado['Cuenta_A'] == 511086000))]



        df_filtrado = df_filtrado[~((df_filtrado['Proyecto_A'] == 2001) & (df_filtrado['Cuenta_A'] == 510100001))]
        df_filtrado = df_filtrado[~((df_filtrado['Proyecto_A'] == 2001) & (df_filtrado['Cuenta_A'] == 510100003))]
        df_filtrado = df_filtrado[~((df_filtrado['Proyecto_A'] == 2001) & (df_filtrado['Cuenta_A'] == 510100004))]
        df_filtrado = df_filtrado[~((df_filtrado['Proyecto_A'] == 2001) & (df_filtrado['Cuenta_A'] == 510100005))]
        df_filtrado = df_filtrado[~((df_filtrado['Proyecto_A'] == 2001) & (df_filtrado['Cuenta_A'] == 510100010))]
        df_filtrado = df_filtrado[~((df_filtrado['Proyecto_A'] == 2001) & (df_filtrado['Cuenta_A'] == 510100018))]
        df_filtrado = df_filtrado[~((df_filtrado['Proyecto_A'] == 2001) & (df_filtrado['Cuenta_A'] == 510100023))]
        df_filtrado = df_filtrado[~((df_filtrado['Proyecto_A'] == 2001) & (df_filtrado['Cuenta_A'] == 510100040))]
        df_filtrado = df_filtrado[~((df_filtrado['Proyecto_A'] == 2001) & (df_filtrado['Cuenta_A'] == 510100057))]
        df_filtrado = df_filtrado[~((df_filtrado['Proyecto_A'] == 2001) & (df_filtrado['Cuenta_A'] == 510100058))]
        df_filtrado = df_filtrado[~((df_filtrado['Proyecto_A'] == 2001) & (df_filtrado['Cuenta_A'] == 510100012))]
        df_filtrado = df_filtrado[~((df_filtrado['Proyecto_A'] == 2001) & (df_filtrado['Cuenta_A'] == 510100011))]
        df_filtrado = df_filtrado[~((df_filtrado['Proyecto_A'] == 2001) & (df_filtrado['Cuenta_A'] == 510100002))]
        df_filtrado = df_filtrado[~((df_filtrado['Proyecto_A'] == 2001) & (df_filtrado['Cuenta_A'] == 510100070))]
        df_filtrado = df_filtrado[~((df_filtrado['Proyecto_A'] == 2001) & (df_filtrado['Cuenta_A'] == 511086000))]



        df_filtrado = df_filtrado[~((df_filtrado['Proyecto_A'] == 3002) & (df_filtrado['Cuenta_A'] == 510100001))]
        df_filtrado = df_filtrado[~((df_filtrado['Proyecto_A'] == 3002) & (df_filtrado['Cuenta_A'] == 510100003))]
        df_filtrado = df_filtrado[~((df_filtrado['Proyecto_A'] == 3002) & (df_filtrado['Cuenta_A'] == 510100004))]
        df_filtrado = df_filtrado[~((df_filtrado['Proyecto_A'] == 3002) & (df_filtrado['Cuenta_A'] == 510100005))]
        df_filtrado = df_filtrado[~((df_filtrado['Proyecto_A'] == 3002) & (df_filtrado['Cuenta_A'] == 510100010))]
        df_filtrado = df_filtrado[~((df_filtrado['Proyecto_A'] == 3002) & (df_filtrado['Cuenta_A'] == 510100018))]
        df_filtrado = df_filtrado[~((df_filtrado['Proyecto_A'] == 3002) & (df_filtrado['Cuenta_A'] == 510100023))]
        df_filtrado = df_filtrado[~((df_filtrado['Proyecto_A'] == 3002) & (df_filtrado['Cuenta_A'] == 510100040))]
        df_filtrado = df_filtrado[~((df_filtrado['Proyecto_A'] == 3002) & (df_filtrado['Cuenta_A'] == 510100057))]
        df_filtrado = df_filtrado[~((df_filtrado['Proyecto_A'] == 3002) & (df_filtrado['Cuenta_A'] == 510100058))]
        df_filtrado = df_filtrado[~((df_filtrado['Proyecto_A'] == 3002) & (df_filtrado['Cuenta_A'] == 510100012))]
        df_filtrado = df_filtrado[~((df_filtrado['Proyecto_A'] == 3002) & (df_filtrado['Cuenta_A'] == 510100011))]
        df_filtrado = df_filtrado[~((df_filtrado['Proyecto_A'] == 3002) & (df_filtrado['Cuenta_A'] == 510100002))]
        df_filtrado = df_filtrado[~((df_filtrado['Proyecto_A'] == 3002) & (df_filtrado['Cuenta_A'] == 510100070))]
        df_filtrado = df_filtrado[~((df_filtrado['Proyecto_A'] == 3002) & (df_filtrado['Cuenta_A'] == 511086000))]


        df_filtrado = df_filtrado[~((df_filtrado['Proyecto_A'] == 3201) & (df_filtrado['Cuenta_A'] == 510100001))]
        df_filtrado = df_filtrado[~((df_filtrado['Proyecto_A'] == 3201) & (df_filtrado['Cuenta_A'] == 510100003))]
        df_filtrado = df_filtrado[~((df_filtrado['Proyecto_A'] == 3201) & (df_filtrado['Cuenta_A'] == 510100004))]
        df_filtrado = df_filtrado[~((df_filtrado['Proyecto_A'] == 3201) & (df_filtrado['Cuenta_A'] == 510100005))]
        df_filtrado = df_filtrado[~((df_filtrado['Proyecto_A'] == 3201) & (df_filtrado['Cuenta_A'] == 510100010))]
        df_filtrado = df_filtrado[~((df_filtrado['Proyecto_A'] == 3201) & (df_filtrado['Cuenta_A'] == 510100018))]
        df_filtrado = df_filtrado[~((df_filtrado['Proyecto_A'] == 3201) & (df_filtrado['Cuenta_A'] == 510100023))]
        df_filtrado = df_filtrado[~((df_filtrado['Proyecto_A'] == 3201) & (df_filtrado['Cuenta_A'] == 510100040))]
        df_filtrado = df_filtrado[~((df_filtrado['Proyecto_A'] == 3201) & (df_filtrado['Cuenta_A'] == 510100057))]
        df_filtrado = df_filtrado[~((df_filtrado['Proyecto_A'] == 3201) & (df_filtrado['Cuenta_A'] == 510100058))]
        df_filtrado = df_filtrado[~((df_filtrado['Proyecto_A'] == 3201) & (df_filtrado['Cuenta_A'] == 510100012))]
        df_filtrado = df_filtrado[~((df_filtrado['Proyecto_A'] == 3201) & (df_filtrado['Cuenta_A'] == 510100011))]
        df_filtrado = df_filtrado[~((df_filtrado['Proyecto_A'] == 3201) & (df_filtrado['Cuenta_A'] == 510100002))]
        df_filtrado = df_filtrado[~((df_filtrado['Proyecto_A'] == 3201) & (df_filtrado['Cuenta_A'] == 510100070))]
        df_filtrado = df_filtrado[~((df_filtrado['Proyecto_A'] == 3201) & (df_filtrado['Cuenta_A'] == 511086000))]

        mes_nolist = mes_seleccionado[0]



        # Contar los jueves
        jueves = contar_jueves(mes_nolist, año_seleccionado)

        if jueves == 4:
            multiplo_nomina = 1.075
        else:
            multiplo_nomina = 0.86

        df_filtrado.loc[df_filtrado['Categoria_A'] == 'NOMINA OPERADORES', 'Neto_A'] *= multiplo_nomina
        df_filtrado.loc[df_filtrado['Categoria_A'] == 'NOMINA ADMINISTRATIVOS', 'Neto_A'] *= multiplo_nomina
        
        df_filtrado = pd.concat([df_filtrado, df_provisiones], ignore_index=True)

    elif opcion == "Incluir provisiones cierre de mes(ingresos gaby)":
        st.write("Las provisiones de cierre de mes serán incluidas en el DataFrame.")

        f_filtrado = df_filtrado[~((df_filtrado['Categoria_A'] == 'FLETES') & (df_filtrado['Proyecto_A'].isin([1003, 7806])))]
        df_filtrado = df_filtrado[~(df_filtrado['CeCo_A'] == 50)]

        df_filtrado = df_filtrado[~((df_filtrado['Proyecto_A'] == 1001) & (df_filtrado['Cuenta_A'] == 510100001))]

        df_filtrado = df_filtrado[~((df_filtrado['Proyecto_A'] == 1003) & (df_filtrado['Cuenta_A'] == 510100001))]

        df_filtrado = df_filtrado[~((df_filtrado['Proyecto_A'] == 5001) & (df_filtrado['Cuenta_A'] == 510100001))]

        df_filtrado = df_filtrado[~((df_filtrado['Proyecto_A'] == 2001) & (df_filtrado['Cuenta_A'] == 510100001))]

        df_filtrado = df_filtrado[~((df_filtrado['Proyecto_A'] == 2003) & (df_filtrado['Cuenta_A'] == 510100001))]

        df_filtrado = df_filtrado[~((df_filtrado['Proyecto_A'] == 3002) & (df_filtrado['Cuenta_A'] == 510100001))]

        df_filtrado = df_filtrado[~((df_filtrado['Proyecto_A'] == 3201) & (df_filtrado['Cuenta_A'] == 510100001))]

        df_provisiones = df_provisiones[df_provisiones['Categoria_A'] != 'INGRESO']

        cuentas__quitar_provisiones = [510100003, 510100004, 510100005, 510100010, 510100018 , 510100023 , 510100040, 510100056, 511121000,
                                       510100057, 510100058, 510100012, 510100011, 510100002,  511086000, 510100070, 511082000, 510100039]
        df_provisiones = df_provisiones[~df_provisiones['Cuenta_A'].isin(cuentas__quitar_provisiones)]
        df_provisiones = df_provisiones[~((df_provisiones['Categoria_A'] == 'FLETES') & (~df_provisiones['Proyecto_A'].isin([1003,7806])))]
        df_provisiones = df_provisiones[df_provisiones['Categoria_A'] != 'CASETAS']
        
        df_filtrado = pd.concat([df_filtrado, df_provisiones], ignore_index=True)
        
    
    elif opcion == "Incluir provisiones cierre de mes (ingresos nosotros)":
        st.write("Las provisiones de cierre de mes serán incluidas en el DataFrame.")

        df_filtrado = df_filtrado[~(df_filtrado['Categoria_A'] == 'INGRESO')]

        df_filtrado = df_filtrado[~((df_filtrado['Categoria_A'] == 'FLETES') & (df_filtrado['Proyecto_A'].isin([1003, 7806])))]
        df_filtrado = df_filtrado[~(df_filtrado['Categoria_A'] == 'FLETES')]

        df_filtrado = df_filtrado[~(df_filtrado['CeCo_A'] == 50)]

        df_filtrado = df_filtrado[~((df_filtrado['Proyecto_A'] == 1001) & (df_filtrado['Cuenta_A'] == 510100001))]

        df_filtrado = df_filtrado[~((df_filtrado['Proyecto_A'] == 1003) & (df_filtrado['Cuenta_A'] == 510100001))]

        df_filtrado = df_filtrado[~((df_filtrado['Proyecto_A'] == 5001) & (df_filtrado['Cuenta_A'] == 510100001))]

        df_filtrado = df_filtrado[~((df_filtrado['Proyecto_A'] == 2001) & (df_filtrado['Cuenta_A'] == 510100001))]

        df_filtrado = df_filtrado[~((df_filtrado['Proyecto_A'] == 2003) & (df_filtrado['Cuenta_A'] == 510100001))]

        df_filtrado = df_filtrado[~((df_filtrado['Proyecto_A'] == 3002) & (df_filtrado['Cuenta_A'] == 510100001))]

        df_filtrado = df_filtrado[~((df_filtrado['Proyecto_A'] == 3201) & (df_filtrado['Cuenta_A'] == 510100001))]

        cuentas__quitar_provisiones = [510100003, 510100004, 510100005, 510100010, 510100018 , 510100023 , 510100040, 510100056, 511121000,
                                       510100057, 510100058, 510100012, 510100011, 510100002,  511086000, 510100070, 511082000, 510100039]
        df_provisiones = df_provisiones[~df_provisiones['Cuenta_A'].isin(cuentas__quitar_provisiones)]
        df_provisiones = df_provisiones[~((df_provisiones['Categoria_A'] == 'FLETES') & (~df_provisiones['Proyecto_A'].isin([1003,7806])))]
        df_provisiones = df_provisiones[df_provisiones['Categoria_A'] != 'CASETAS']

        
        df_filtrado = pd.concat([df_filtrado, df_provisiones], ignore_index=True)

    if not df_base.empty and opcion != "data frame del sistema":
        mes_eliminar = df_filtrado['Mes_A'].unique()
        df_base = df_base[~df_base['Mes_A'].isin(mes_eliminar)]
        df_filtrado = pd.concat([df_base, df_filtrado], ignore_index=True)
        


# Obtener filas problemáticas
invalid_rows = get_invalid_rows(df_filtrado)

# Mostrar filas problemáticas en Streamlit
if not invalid_rows.empty:
    st.warning("Se encontraron filas con valores problemáticos:")
    st.write(invalid_rows)
else:
    st.success("No se encontraron valores problemáticos en el DataFrame.")

output = BytesIO()
with pd.ExcelWriter(output, engine='xlsxwriter') as writer:
        df_filtrado.to_excel(writer, index=False, sheet_name='Reporte_XTR')
        output.seek(0)  # Regresar el puntero al inicio del archivo en memoria
st.write("### DataFrame actual:")
st.write(df_filtrado)
df_copiado = df_filtrado.copy()

# Botón para subir datos

# Generar un archivo Excel desde un DataFrame
def generar_excel(dataframe):
    output = BytesIO()
    with pd.ExcelWriter(output, engine='xlsxwriter') as writer:
        dataframe.to_excel(writer, index=False, sheet_name='Reporte_XTR')
    output.seek(0)  # Regresar el puntero al inicio del archivo en memoria
    return output

archivo_excel = generar_excel(df_copiado)

# Botón para descargar el archivo Excel
st.download_button(
    label="📥 Descargar Excel",
    data=archivo_excel,
    file_name="datos.xlsx",
    mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
)
if len(mes_seleccionado) == 1:
    if st.button("Subir a Google Sheets"):
        # Conectar a Google Sheets
        sheet = authenticate_gsheet(json_file, spreadsheet_name)
        
        # Subir el DataFrame
        upload_to_gsheet_optimized(sheet, df_copiado)
    if st.button("actualizar fecha"):
        spreadsheet_id = "1loPFsSZ3agTRuUAYWCDXFYtGMjvp6lh8"
        archivo = "Fecha de actualizacion"
        hooja = "https://docs.google.com/spreadsheets/d/1loPFsSZ3agTRuUAYWCDXFYtGMjvp6lh8/edit?usp=sharing&ouid=101175782095158984544&rtpof=true&sd=true"
        actualizacion_fecha(hooja)
