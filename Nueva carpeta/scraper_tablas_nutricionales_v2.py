# -*- coding: utf-8 -*-
import requests
from bs4 import BeautifulSoup
import json
import os
from datetime import datetime
import time # Opcional, para pausas entre solicitudes

# Encabezados para simular un navegador
headers = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, Gecko) Chrome/123.0.0.0 Safari/537.36"
}

def find_key_in_json(data_item, target_key):
    """Busca recursivamente una clave en un diccionario o lista anidada (similar a JSON)."""
    if isinstance(data_item, dict):
        if target_key in data_item:
            return data_item[target_key]
        for key, value in data_item.items():
            result = find_key_in_json(value, target_key)
            if result is not None:
                return result
    elif isinstance(data_item, list):
        for item_element in data_item:
            result = find_key_in_json(item_element, target_key)
            if result is not None:
                return result
    return None

def extract_nutritional_info_from_product_page(product_url, session):
    """Extrae la información de 'nutritional_tables_sets' de la página de un producto."""
    print(f"Procesando URL del producto: {product_url}")
    try:
        response = session.get(product_url, headers=headers, timeout=30)
        response.raise_for_status()  # Lanza una excepción para errores HTTP (4xx o 5xx)
        soup = BeautifulSoup(response.text, "html.parser")
        script_tag = soup.find("script", {"id": "__NEXT_DATA__"})

        if not (script_tag and script_tag.string):
            print(f"No se encontró __NEXT_DATA__ o estaba vacío para {product_url}")
            return None

        data = json.loads(script_tag.string)
        nutritional_info = None
        
        # Intentar extraer de la ruta pageProps.product.products[0]...
        page_props = data.get("props", {}).get("pageProps", {})
        if page_props:
            product_section = page_props.get("product", {})
            if product_section:
                products_list = product_section.get("products", [])
                if products_list and isinstance(products_list, list) and len(products_list) > 0:
                    first_product = products_list[0]
                    if isinstance(first_product, dict):
                        # Primero, buscar directamente en el objeto del producto
                        nutritional_info = first_product.get("nutritional_tables_sets")
                        
                        # Si no está ahí, buscar dentro de su 'item'
                        if nutritional_info is None:
                            item_data = first_product.get("item")
                            if item_data and isinstance(item_data, dict):
                                nutritional_info = item_data.get("nutritional_tables_sets")
            
            if nutritional_info is not None:
                print(f"Tabla nutricional encontrada en pageProps.product.products[0] para {product_url}")
                return nutritional_info

            # Si no se encontró en la ruta principal, intentar buscar en dehydratedState.queries
            dehydrated_state = page_props.get("dehydratedState", {})
            queries = dehydrated_state.get("queries", [])
            if queries:
                for query in queries:
                    query_data_root = query.get("state", {}).get("data", {})
                    if query_data_root:
                        # Buscar recursivamente la clave dentro de los datos de esta query
                        # Esto es más robusto ya que la estructura interna de query_data_root puede variar
                        nutritional_info_in_query = find_key_in_json(query_data_root, "nutritional_tables_sets")
                        if nutritional_info_in_query is not None:
                            print(f"Tabla nutricional encontrada en dehydratedState.queries para {product_url}")
                            return nutritional_info_in_query
        
        # Si después de todos los intentos no se encontró
        print(f"No se encontró 'nutritional_tables_sets' en las rutas esperadas para {product_url}")
        # Guardar __NEXT_DATA__ completo para depuración
        output_dir_debug = "Nutritional Data Unimarc"
        os.makedirs(output_dir_debug, exist_ok=True)
        sanitized_url_part = product_url.split("/")[-1].replace("?", "_").replace("=", "_")
        if not sanitized_url_part and len(product_url.split("/")) > 1:
            sanitized_url_part = product_url.split("/")[-2].replace("?", "_").replace("=", "_")
        if not sanitized_url_part: # Fallback si la URL es muy simple
            sanitized_url_part = "unknown_product"
            
        debug_filename = f"debug_next_data_no_nutritional_{sanitized_url_part}_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
        debug_filepath = os.path.join(output_dir_debug, debug_filename)
        try:
            with open(debug_filepath, "w", encoding="utf-8") as f_debug:
                json.dump(data, f_debug, ensure_ascii=False, indent=4)
            print(f"__NEXT_DATA__ completo guardado para depuración en: {debug_filepath}")
        except Exception as e_write:
            print(f"Error al guardar archivo de depuración {debug_filepath}: {e_write}")
        return None

    except requests.exceptions.RequestException as e:
        print(f"Error en la solicitud para {product_url}: {e}")
        return None
    except json.JSONDecodeError:
        print(f"Error al decodificar JSON de __NEXT_DATA__ para {product_url}. El contenido podría no ser JSON válido.")
        # Opcional: guardar response.text para análisis
        return None
    except Exception as e:
        print(f"Error inesperado procesando {product_url}: {e}")
        # import traceback
        # traceback.print_exc() # Para depuración más detallada
        return None

def main_nutritional_scraper():
    input_urls_dir = "Detail URLs Unimarc"
    output_nutritional_dir = "Nutritional Data Unimarc"
    os.makedirs(output_nutritional_dir, exist_ok=True)

    if not os.path.isdir(input_urls_dir):
        print(f"El directorio de entrada '{input_urls_dir}' no existe. Asegúrese de que el primer script lo haya creado y poblado.")
        return

    url_files = [f for f in os.listdir(input_urls_dir) if f.startswith("detalle_urls_") and f.endswith(".txt")]
    if not url_files:
        print(f"No se encontraron archivos de URLs de detalle (ej: detalle_urls_*.txt) en '{input_urls_dir}'.")
        print(f"Asegúrese de ejecutar primero el scraper principal modificado para generar estos archivos.")
        return

    all_product_urls = []
    for url_file_name in url_files:
        url_file_path = os.path.join(input_urls_dir, url_file_name)
        print(f"Leyendo URLs desde: {url_file_path}")
        try:
            with open(url_file_path, 'r', encoding='utf-8') as f:
                urls_from_file = [line.strip() for line in f if line.strip() and line.strip().startswith("http")]
                all_product_urls.extend(urls_from_file)
                print(f"Se cargaron {len(urls_from_file)} URLs válidas desde {url_file_name}")
        except Exception as e:
            print(f"Error al leer el archivo {url_file_path}: {e}")
            continue

    if not all_product_urls:
        print("No se cargaron URLs de productos válidas para procesar.")
        return

    unique_product_urls = sorted(list(set(all_product_urls)))
    print(f"Total de URLs de productos únicas a procesar: {len(unique_product_urls)}")

    results = []
    with requests.Session() as session: # Usar una sesión para eficiencia
        for i, product_url in enumerate(unique_product_urls):
            print(f"Procesando {i+1}/{len(unique_product_urls)}: {product_url}")
            nutritional_data = extract_nutritional_info_from_product_page(product_url, session)
            # Guardar incluso si nutritional_data es None o una lista vacía, para saber qué URLs se procesaron
            # El usuario pidió guardar el JSON de la tabla, por lo que si es None, no se guarda esa entrada.
            # Si la tabla es una lista vacía (ej. nutritional_tables_sets: []), sí se guarda.
            if nutritional_data is not None: 
                results.append({
                    "url_producto": product_url,
                    "tabla_nutricional_sets": nutritional_data
                })
            # time.sleep(0.1) # Pausa opcional para ser cortés con el servidor

    if results:
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        output_filename = f"tablas_nutricionales_unimarc_{timestamp}.json"
        output_filepath = os.path.join(output_nutritional_dir, output_filename)
        
        try:
            with open(output_filepath, "w", encoding="utf-8") as f_out:
                json.dump(results, f_out, ensure_ascii=False, indent=4)
            print(f"\nProceso completado. {len(results)} registros de tablas nutricionales guardados en: {output_filepath}")
        except Exception as e:
            print(f"Error al guardar el archivo JSON de resultados {output_filepath}: {e}")
    else:
        print("\nNo se pudo extraer ninguna tabla nutricional o todas las URLs fallaron / no contenían la tabla.")

if __name__ == "__main__":
    main_nutritional_scraper()

