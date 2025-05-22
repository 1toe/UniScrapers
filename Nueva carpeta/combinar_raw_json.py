import os
import json
import glob
from datetime import datetime

'''
SCRIPT PARA COMBINAR ARCHIVOS JSON CRUDOS (__NEXT_DATA__) EN UN SOLO ARCHIVO JSON.
Este script busca todos los archivos JSON en la carpeta de JSONs crudos de productos,
los lee, valida y los combina en un único archivo JSON que contiene un diccionario
de todos los objetos __NEXT_DATA__ individuales bajo la clave "datos".
'''

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
RAW_JSON_INPUT_DIR = os.path.join(BASE_DIR, "Resultados_Unimarc", "RAW_JSON") # Carpeta de entrada modificada
OUTPUT_DIR = os.path.join(BASE_DIR, "Resultados JSON Unificados") # Carpeta de salida modificada

def generar_timestamp():
    """Genera un timestamp único para nombrar archivos"""
    return datetime.now().strftime("%Y%m%d_%H%M%S")

def crear_directorio_salida():
    """Crea el directorio de salida si no existe"""
    os.makedirs(OUTPUT_DIR, exist_ok=True)
    print(f"Directorio de salida creado/verificado: {OUTPUT_DIR}")

def listar_archivos_json():
    """Lista todos los archivos JSON disponibles en la carpeta de entrada"""
    if not os.path.isdir(RAW_JSON_INPUT_DIR):
        print(f"ERROR: El directorio de entrada '{RAW_JSON_INPUT_DIR}' no existe. Verifica la ruta.")
        return []
        
    patron_busqueda = os.path.join(RAW_JSON_INPUT_DIR, "raw_json_producto_*.json") # Patrón específico si aplica
    archivos = glob.glob(patron_busqueda)
    
    if not archivos:
        # Intenta un patrón más genérico si el específico no encuentra nada
        patron_busqueda_generico = os.path.join(RAW_JSON_INPUT_DIR, "*.json")
        archivos = glob.glob(patron_busqueda_generico)
        if archivos:
            print(f"Advertencia: No se encontraron archivos con el patrón '{patron_busqueda}'.")
            print(f"Se encontraron {len(archivos)} archivos JSON con el patrón genérico '{patron_busqueda_generico}'.")
        else:
            print(f"No se encontraron archivos JSON en '{RAW_JSON_INPUT_DIR}' con los patrones probados.")
            return []
    else:
        print(f"Se encontraron {len(archivos)} archivos JSON con el patrón '{patron_busqueda}' en '{RAW_JSON_INPUT_DIR}'.")
    return archivos

def validar_json_y_cargar(ruta_archivo):
    """Valida que un archivo contenga JSON válido y lo carga."""
    try:
        with open(ruta_archivo, 'r', encoding='utf-8') as f:
            data = json.load(f)
        # Opcional: verificar si es un objeto (diccionario en Python)
        if not isinstance(data, dict):
            print(f"Advertencia: El archivo {os.path.basename(ruta_archivo)} contiene JSON válido, pero no es un objeto (diccionario). Se incluirá tal cual.")
        return data
    except json.JSONDecodeError as e:
        print(f"Error de decodificación JSON en archivo {os.path.basename(ruta_archivo)}: {e}. Archivo excluido.")
        return None
    except Exception as e:
        print(f"Error al leer o procesar archivo {os.path.basename(ruta_archivo)}: {e}. Archivo excluido.")
        return None

def combinar_raw_archivos_json():
    """Combina el contenido de todos los archivos JSON válidos en un diccionario bajo la clave 'datos'."""
    archivos = listar_archivos_json()
    if not archivos:
        return None
    
    datos_combinados = {"datos": {}}
    archivos_procesados = 0
    
    for archivo_path in archivos:
        datos_json = validar_json_y_cargar(archivo_path)
        if datos_json is not None:
            # Usar el nombre del archivo (sin extensión) como clave
            nombre_archivo = os.path.splitext(os.path.basename(archivo_path))[0]
            datos_combinados["datos"][nombre_archivo] = datos_json
            archivos_procesados += 1
            print(f"Procesado y añadido: {os.path.basename(archivo_path)}")
        else:
            print(f"Archivo excluido debido a errores: {os.path.basename(archivo_path)}")
            
    if archivos_procesados == 0:
        print("No se pudo cargar datos válidos de ningún archivo JSON.")
        return None
    
    print(f"Total de archivos JSON combinados: {archivos_procesados}")
    return datos_combinados

def guardar_json_combinado(datos_combinados):
    """Guarda el JSON combinado (diccionario con clave 'datos') en un archivo."""
    if not datos_combinados or "datos" not in datos_combinados or not datos_combinados["datos"]:
        print("No hay datos para guardar o la estructura es incorrecta.")
        return None
    
    crear_directorio_salida()
    
    timestamp = generar_timestamp()
    # Nombre de archivo más descriptivo para este tipo de combinación
    nombre_archivo = f"json_combinado_{timestamp}.json"
    ruta_completa = os.path.join(OUTPUT_DIR, nombre_archivo)
    
    try:
        with open(ruta_completa, 'w', encoding='utf-8') as f:
            json.dump(datos_combinados, f, ensure_ascii=False, indent=4)
        print(f"JSON combinado guardado exitosamente en: {ruta_completa}")
        return ruta_completa
    except Exception as e:
        print(f"Error al guardar el JSON combinado: {e}")
        return None

def main():
    """Función principal"""
    print("\n" + "="*60)
    print("COMBINADOR DE ARCHIVOS JSON CRUDOS (__NEXT_DATA__)")
    print(" Fusiona múltiples archivos JSON en formato compatible ")
    print("="*60)
    
    crear_directorio_salida()
    
    print(f"\nBuscando archivos JSON en: {RAW_JSON_INPUT_DIR}...")
    datos_combinados = combinar_raw_archivos_json()
    
    if datos_combinados:
        ruta_guardado = guardar_json_combinado(datos_combinados)
        if ruta_guardado:
            print("\n" + "="*60)
            print(f"PROCESO DE COMBINACIÓN COMPLETADO")
            print(f"Total de objetos JSON combinados: {len(datos_combinados['datos'])}")
            print(f"Archivo combinado guardado en: {ruta_guardado}")
            print("="*60)
    else:
        print("\n" + "="*60)
        print("PROCESO DE COMBINACIÓN FINALIZADO SIN RESULTADOS")
        print("No se pudieron combinar los archivos JSON crudos.")
        print("Verifique el directorio de entrada y los archivos.")
        print("="*60)

if __name__ == "__main__":
    main()