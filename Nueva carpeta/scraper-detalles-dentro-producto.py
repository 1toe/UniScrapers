import requests
from bs4 import BeautifulSoup
import json
import os
from datetime import datetime
import time
import random

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

def extract_product_details(soup, url):
    """Extrae detalles de un producto individual"""
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
                
                if product_data:
                    # Extraer información básica del producto
                    product_details.update({
                        "nombre": product_data.get("nameComplete"),
                        "nombre_corto": product_data.get("name"),
                        "marca": product_data.get("brand"),
                        "sku": product_data.get("productId") or product_data.get("itemId"),
                        "categoria": product_data.get("categoryId"),
                        "descripcion": product_data.get("description")
                    })
                    
                    # Extraer precio y disponibilidad
                    if "items" in product_data:
                        items = product_data.get("items", [])
                        if items and len(items) > 0:
                            item = items[0]
                            sellers = item.get("sellers", [])
                            if sellers and len(sellers) > 0:
                                product_details["precio"] = sellers[0].get("commertialOffer", {}).get("Price")
                                product_details["precio_lista"] = sellers[0].get("commertialOffer", {}).get("ListPrice")
                                product_details["disponible"] = sellers[0].get("commertialOffer", {}).get("AvailableQuantity", 0) > 0
                    
                    # Extraer imágenes
                    images = product_data.get("images", [])
                    if images and len(images) > 0:
                        product_details["imagenes"] = images
                        product_details["imagen_principal"] = images[0]
                    
                    # Extraer especificaciones
                    specs = []
                    if "allSpecifications" in product_data:
                        specs_list = product_data.get("allSpecifications", [])
                        for spec_name in specs_list:
                            spec_values = product_data.get("specificationGroups", [])
                            for group in spec_values:
                                for spec in group.get("specifications", []):
                                    if spec.get("name") == spec_name:
                                        specs.append({
                                            "nombre": spec_name,
                                            "valor": spec.get("values", [])[0] if spec.get("values") else None
                                        })
                    product_details["especificaciones"] = specs
                    
                    # Extraer información nutricional (si existe)
                    if "Información nutricional" in product_data.get("allSpecifications", []):
                        product_details["info_nutricional"] = True
                    
                    # Extraer sellos de advertencia
                    sellos = []
                    if "warnings" in product_data:
                        warnings = product_data.get("warnings", [])
                        for warning in warnings:
                            sellos.append(warning.get("name"))
                    product_details["sellos_advertencia"] = sellos
                    
                else:
                    print("No se encontraron datos del producto en la estructura JSON.")
            except json.JSONDecodeError:
                print("Error al decodificar el JSON de __NEXT_DATA__.")
            except (KeyError, IndexError, TypeError) as e:
                print(f"Error al navegar la estructura JSON de __NEXT_DATA__: {e}")
        else:
            print("La etiqueta <script id='__NEXT_DATA__'> no tiene contenido.")
    else:
        print("No se encontró la etiqueta <script id='__NEXT_DATA__'. No se pueden extraer datos del producto.")

    # También extraer datos de las etiquetas HTML directamente si es necesario
    try:
        title_tag = soup.find("title")
        if title_tag:
            product_details["title_tag"] = title_tag.text.strip()
        
        meta_description = soup.find("meta", {"name": "description"})
        if meta_description and "content" in meta_description.attrs:
            product_details["meta_description"] = meta_description["content"]
    except Exception as e:
        print(f"Error al extraer datos HTML adicionales: {e}")

    return product_details

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
                
            # Extraer detalles del producto
            product_details = extract_product_details(soup, url)
            
            if product_details:
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
    
    # Guardar resultados en formato JSON
    if productos_detalles:
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        json_filename = f"detalles_productos_unimarc_{len(productos_detalles)}_productos_{timestamp}.json"
        json_path = os.path.join(json_folder, json_filename)
        
        with open(json_path, "w", encoding="utf-8") as f_json:
            json.dump(productos_detalles, f_json, ensure_ascii=False, indent=4)
        
        print(f"\nTotal de productos detallados extraídos: {len(productos_detalles)}")
        print(f"Archivo JSON guardado como: {json_path}")
    else:
        print("\nNo se lograron extraer detalles de productos.")

if __name__ == "__main__":
    main()
