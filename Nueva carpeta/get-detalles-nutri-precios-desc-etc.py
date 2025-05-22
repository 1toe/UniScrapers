import requests
from bs4 import BeautifulSoup
import json
import os
from datetime import datetime
import time
import random

'''
Este script extrae información detallada de productos de Unimarc a partir de URLs de productos individuales.

Funcionalidades principales:
1. Lee URLs de productos desde un archivo de texto
2. Para cada URL:
   - Descarga el contenido HTML de la página
   - Extrae el JSON completo de __NEXT_DATA__ y lo guarda en un archivo
   - Extrae información específica como datos nutricionales y descripción del producto
   - Guarda la información de cada producto en un archivo JSON individual
3. Consolida todos los datos en un archivo JSON único con todos los productos

El script se enfoca principalmente en extraer:
- Información nutricional de los productos
- Descripciones completas de productos
- Datos básicos como nombre, SKU, marca, etc.

Los archivos se guardan en carpetas organizadas para:
- HTML descargado (HTML_Productos_Unimarc)
- JSON original completo (__NEXT_DATA__) (RAW_JSON_Productos_Unimarc)
- JSON procesado por producto (JSON_Individual_Productos_Unimarc)
- JSON consolidado con todos los productos (JSON_Productos_Unimarc)
'''

# Encabezados para simular un navegador
headers = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/123.0.0.0 Safari/537.36"
}

def leer_urls_desde_archivo(archivo):
    """Lee las URLs de productos desde un archivo de texto"""
    urls = []
    try:
        with open(archivo, 'r', encoding='utf-8') as file:
            for linea in file:
                linea = linea.strip()
                if linea and not linea.startswith('//'):
                    urls.append(linea)
        
        if urls:
            print(f"URLs de productos cargadas exitosamente desde {archivo}: {len(urls)} URLs")
        else:
            print(f"No se encontraron URLs válidas en {archivo}")
    except FileNotFoundError:
        print(f"El archivo {archivo} no existe.")
        return None
    except Exception as e:
        print(f"Error al leer el archivo de URLs: {e}")
        return None
    
    return urls

def extract_and_save_raw_json(soup, product_id, timestamp):
    """Extrae y guarda el JSON completo de __NEXT_DATA__"""
    script_tag = soup.find("script", {"id": "__NEXT_DATA__"})
    
    if not script_tag or not script_tag.string:
        print("No se encontró el JSON __NEXT_DATA__ o está vacío.")
        return None
    
    try:
        # Crear directorio para guardar los JSONs
        json_raw_folder = "RAW_JSON_Productos_Unimarc"
        os.makedirs(json_raw_folder, exist_ok=True)
        
        # Formatear el JSON para mejor legibilidad
        json_data = json.loads(script_tag.string)
        json_formatted = json.dumps(json_data, ensure_ascii=False, indent=4)
        
        # Guardar el JSON completo
        json_filename = f"raw_json_producto_{product_id}_{timestamp}.json"
        json_path = os.path.join(json_raw_folder, json_filename)
        
        with open(json_path, "w", encoding="utf-8") as f:
            f.write(json_formatted)
        
        print(f"JSON completo de __NEXT_DATA__ guardado como: {json_path}")
        return json_data
    except json.JSONDecodeError:
        print("Error al decodificar el JSON de __NEXT_DATA__.")
        return None
    except Exception as e:
        print(f"Error al guardar el JSON completo: {e}")
        return None

def extract_product_details(soup, url):
    """Extrae detalles de un producto individual enfocándose en información nutricional y descripción"""
    product_details = {
        "url": url,
        "fecha_extraccion": datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    }
    
    print(f"Extrayendo datos detallados del producto desde {url}")
    script_tag = soup.find("script", {"id": "__NEXT_DATA__"})

    if script_tag:
        json_data_string = script_tag.string
        if json_data_string:
            try:
                data = json.loads(json_data_string)
                
                # Extraer solo información básica, nutricional y descripción completa
                
                # Intentar extraer detalles del producto desde diferentes rutas en el JSON
                product_data = None
                pageProps = data.get("props", {}).get("pageProps", {})
                
                # Buscar en diferentes ubicaciones posibles del JSON
                if "product" in pageProps:
                    product_data = pageProps.get("product")
                elif "dehydratedState" in pageProps:
                    queries = pageProps.get("dehydratedState", {}).get("queries", [])
                    for query in queries:
                        if query.get("state", {}).get("data", {}).get("product"):
                            product_data = query["state"]["data"]["product"]
                            break
                
                # Extraer información nutricional y descripción desde dehydratedState
                nutritional_info = {}
                full_description = None
                
                if "dehydratedState" in pageProps:
                    queries = pageProps.get("dehydratedState", {}).get("queries", [])
                    for query in queries:
                        state_data = query.get("state", {}).get("data", {})
                        
                        # Buscar datos nutricionales en diferentes estructuras
                        if "data" in state_data and "response" in state_data.get("data", {}):
                            response_data = state_data["data"]["response"]
                            
                            # Extraer descripción completa si existe
                            if "full_description" in response_data:
                                full_description = response_data["full_description"]
                            
                            # Extraer información de ingredientes/nutricional
                            if "nutritional_tables_sets" in response_data:
                                nutritional_tables_sets = response_data["nutritional_tables_sets"]
                                nutritional_info["nutritional_tables_sets"] = nutritional_tables_sets

                # Información básica del producto
                if product_data:
                    product_details["nombre"] = product_data.get("nameComplete") or product_data.get("name")
                    product_details["sku"] = product_data.get("productId") or product_data.get("itemId")
                    product_details["descripcion"] = product_data.get("description")
                    
                    # Buscar información nutricional en allSpecifications
                    if "allSpecifications" in product_data:
                        specs = product_data.get("allSpecifications", [])
                        if "Información nutricional" in specs:
                            nutritional_info["tiene_informacion_nutricional"] = True
                            
                            # Buscar los valores específicos de información nutricional
                            spec_groups = product_data.get("specificationGroups", [])
                            for group in spec_groups:
                                for spec in group.get("specifications", []):
                                    if spec.get("name") == "Información nutricional":
                                        nutritional_info["valores"] = spec.get("values", [])
                                        break
                
                # Guardar la información nutricional encontrada
                if nutritional_info:
                    product_details["informacion_nutricional"] = nutritional_info
                
                # Guardar descripción completa
                if full_description:
                    product_details["descripcion_completa"] = full_description
                
            except json.JSONDecodeError:
                print("Error al decodificar el JSON de __NEXT_DATA__.")
            except (KeyError, IndexError, TypeError) as e:
                print(f"Error al navegar la estructura JSON de __NEXT_DATA__: {e}")
        else:
            print("La etiqueta <script id='__NEXT_DATA__'> no tiene contenido.")
    else:
        print("No se encontró la etiqueta <script id='__NEXT_DATA__'. No se pueden extraer datos del producto.")

    return product_details

def save_individual_product_json(product_details, product_id):
    """Guarda los detalles de un producto individual en un archivo JSON separado"""
    try:
        # Crear directorio para guardar los JSONs individuales
        json_individual_folder = "JSON_Individual_Productos_Unimarc"
        os.makedirs(json_individual_folder, exist_ok=True)
        
        # Usar timestamp para evitar sobreescribir archivos con el mismo ID
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        
        # Nombre del archivo con el ID del producto
        json_filename = f"producto_{product_id}_{timestamp}.json"
        json_path = os.path.join(json_individual_folder, json_filename)
        
        # Guardar los detalles en formato JSON
        with open(json_path, "w", encoding="utf-8") as f:
            json.dump(product_details, f, ensure_ascii=False, indent=4)
        
        print(f"Detalles del producto guardados en archivo individual: {json_path}")
        return True
    except Exception as e:
        print(f"Error al guardar archivo JSON individual para producto {product_id}: {e}")
        return False

def scrape_product_details(urls_list):
    """Procesa cada URL de producto y extrae sus detalles"""
    all_products_details = []
    total_urls = len(urls_list)
    
    for idx, url in enumerate(urls_list, 1):
        try:
            print(f"\n[{idx}/{total_urls}] Procesando URL: {url}")
            response = requests.get(url, headers=headers)
            
            if response.status_code != 200:
                print(f"Error al acceder a la URL {url}: {response.status_code}")
                continue

            soup = BeautifulSoup(response.text, "html.parser")
            
            # Guardar el HTML para depuración si es necesario
            product_id = url.split("/")[-2] if url.endswith("/p") else url.split("/")[-1]
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            html_folder = "HTML_Productos_Unimarc"
            os.makedirs(html_folder, exist_ok=True)
            
            html_filename = f"producto_{product_id}_{timestamp}.html"
            html_path = os.path.join(html_folder, html_filename)
            
            with open(html_path, "w", encoding="utf-8") as f:
                f.write(soup.prettify())
            
            # Extraer y guardar el JSON completo de __NEXT_DATA__
            extract_and_save_raw_json(soup, product_id, timestamp)
                
            # Extraer detalles del producto
            product_details = extract_product_details(soup, url)
            
            if product_details:
                # Guardar en archivo JSON individual para este producto
                save_individual_product_json(product_details, product_id)
                
                # También añadir al listado completo
                all_products_details.append(product_details)
                print(f"Detalles extraídos para producto: {product_details.get('nombre', 'Nombre no disponible')}")
            
            # Espera aleatoria para evitar bloqueos
            wait_time = random.uniform(1.0, 3.0)
            print(f"Esperando {wait_time:.2f} segundos antes de la siguiente solicitud...")
            time.sleep(wait_time)
            
        except Exception as e:
            print(f"Error al procesar la URL {url}: {e}")
    
    return all_products_details

def main():
    # Crear directorios necesarios
    json_folder = "JSON_Productos_Unimarc"
    os.makedirs(json_folder, exist_ok=True)
    
    # Definir archivo de entrada
    archivo_urls = "Detail URLs Unimarc/urls-productos.txt"
    
    # Leer URLs de productos
    urls_productos = leer_urls_desde_archivo(archivo_urls)
    
    if not urls_productos:
        print("No se pudieron cargar las URLs de productos. Verifique el archivo.")
        return
    
    # Extraer detalles de cada producto
    print(f"\n{'='*50}")
    print(f"Iniciando extracción de detalles para {len(urls_productos)} productos")
    print(f"{'='*50}")
    
    productos_detalles = scrape_product_details(urls_productos)
    
    # Guardar resultados consolidados en formato JSON (opcional, ya que ahora cada producto tiene su propio archivo)
    if productos_detalles:
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        json_filename = f"detalles_productos_unimarc_{len(productos_detalles)}_productos_{timestamp}.json"
        json_path = os.path.join(json_folder, json_filename)
        
        with open(json_path, "w", encoding="utf-8") as f_json:
            json.dump(productos_detalles, f_json, ensure_ascii=False, indent=4)
        
        print(f"\nTotal de productos detallados extraídos: {len(productos_detalles)}")
        print(f"Archivo JSON consolidado guardado como: {json_path}")
        print(f"Además, cada producto ha sido guardado en su propio archivo JSON en la carpeta JSON_Individual_Productos_Unimarc")
    else:
        print("\nNo se lograron extraer detalles de productos.")

if __name__ == "__main__":
    main()
