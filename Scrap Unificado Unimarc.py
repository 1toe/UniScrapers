# main_supremo_todo_scraper_unimarc.py

import requests
from bs4 import BeautifulSoup
import json
import os
from datetime import datetime
import time
import random

'''
SCRIPT UNIFICADO SUPREMO DE SCRAPING UNIMARC

Este script combina todas las funcionalidades de scraping para productos Unimarc:
1.  Generación de URLs de categorías con filtros de sellos alimenticios.
2.  Extracción de listados de productos desde estas URLs, obteniendo URLs de detalle de productos.
3.  Extracción de información detallada de cada producto individual:
    -   Datos básicos (nombre, SKU, marca, descripción, imágenes, especificaciones).
    -   Información nutricional completa (tablas, descripción).
    -   Detalles de precios y promociones.
4.  Guardado de datos:
    -   HTML de páginas de listado y de producto.
    -   JSON crudo (__NEXT_DATA__) de cada producto.
    -   JSON procesado individualmente para cada producto (datos generales, nutricionales, precios).
    -   Un archivo JSON consolidado con todos los datos de productos procesados.
    -   Un archivo TXT con todas las URLs de detalle de productos encontradas.
'''

# Configuración global
HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/123.0.0.0 Safari/537.36"
}

# Directorios para guardar resultados
BASE_DIR = "Resultados_Unimarc_Supremo"
HTML_DIR = os.path.join(BASE_DIR, "HTML_Paginas") # HTML de listados y productos
RAW_JSON_PRODUCTOS_DIR = os.path.join(BASE_DIR, "RAW_JSON_Productos") # __NEXT_DATA__ crudos
JSON_PRODUCTOS_PROCESADOS_DIR = os.path.join(BASE_DIR, "JSON_Productos_Procesados_Individuales") # Info general procesada
JSON_PRECIOS_DIR = os.path.join(BASE_DIR, "JSON_Precios_Individuales") # Info de precios procesada
JSON_NUTRICIONAL_DIR = os.path.join(BASE_DIR, "JSON_Nutricional_Individuales") # Info nutricional procesada
LISTADOS_DIR = os.path.join(BASE_DIR, "Info_Listados") # JSONs de productos por listado, y URLs de detalle

# Archivo de entrada para URLs de categorías base
ARCHIVO_URLS_CATEGORIAS_BASE = "links_categorias_unimarc.txt"

def crear_directorios():
    """Crea la estructura de directorios necesaria para guardar resultados"""
    directorios = [
        BASE_DIR, HTML_DIR, RAW_JSON_PRODUCTOS_DIR, JSON_PRODUCTOS_PROCESADOS_DIR,
        JSON_PRECIOS_DIR, JSON_NUTRICIONAL_DIR, LISTADOS_DIR
    ]
    for directorio in directorios:
        os.makedirs(directorio, exist_ok=True)
    print(f"Estructura de directorios creada/verificada en: {BASE_DIR}")

def generar_timestamp():
    """Genera un timestamp único para nombrar archivos"""
    return datetime.now().strftime("%Y%m%d_%H%M%S")

def espera_aleatoria(min_seg=1.5, max_seg=3.5):
    """Espera un tiempo aleatorio entre solicitudes para evitar bloqueos"""
    wait_time = random.uniform(min_seg, max_seg)
    print(f"Esperando {wait_time:.2f} segundos...")
    time.sleep(wait_time)

def leer_urls_base_categorias(archivo):
    """Lee las URLs base de categorías desde un archivo de texto"""
    urls = []
    try:
        with open(archivo, 'r', encoding='utf-8') as file:
            for linea in file:
                linea = linea.strip()
                if linea and not linea.startswith('//') and "unimarc.cl/category/" in linea:
                    urls.append(linea)
        if urls:
            print(f"URLs de categorías base cargadas ({len(urls)}) desde: {archivo}")
        else:
            print(f"No se encontraron URLs de categorías válidas en {archivo}")
    except FileNotFoundError:
        print(f"ERROR: El archivo de URLs de categorías base '{archivo}' no existe.")
        return None
    except Exception as e:
        print(f"Error al leer el archivo de URLs de categorías base: {e}")
        return None
    return urls

# --- Funciones para generar URLs con filtros de sellos ---
def obtener_filtros_sellos():
    """Obtiene los filtros de sellos alimenticios"""
    return [
        "?warningStamps=sin-sellos",
        "?warningStamps=un-sello",
        "?warningStamps=dos-sellos"
    ]

def generar_urls_listado_con_filtros(urls_base, filtros):
    """Genera nuevas URLs de listado combinando cada URL base con cada filtro"""
    urls_combinadas = []
    for url in urls_base:
        for filtro in filtros:
            if '?' in url: # Si la URL base ya tiene parámetros
                nueva_url = f"{url}&{filtro.lstrip('?')}"
            else:
                nueva_url = f"{url}{filtro}"
            urls_combinadas.append(nueva_url)
    print(f"Generadas {len(urls_combinadas)} URLs de listado con filtros de sellos.")
    return urls_combinadas

# --- Funciones de Scraping de Listados de Productos ---
def get_tipo_sello_from_url(url):
    """Extrae el tipo de sello de la URL de listado"""
    if "warningStamps=sin-sellos" in url: return "sin-sellos"
    if "warningStamps=un-sello" in url: return "un-sello"
    if "warningStamps=dos-sellos" in url: return "dos-sellos"
    return "desconocido"

def get_categoria_from_url(url):
    """Extrae la categoría de la URL de listado"""
    try:
        # https://www.unimarc.cl/category/despensa/aceites-y-vinagres?warningStamps=sin-sellos&page=1
        parts = url.split("/category/")
        if len(parts) > 1:
            categoria_part = parts[1].split("?")[0]
            return categoria_part.replace("/", "_")
    except Exception:
        pass
    return "categoria_desconocida"

def get_total_products_from_listing(soup):
    """Extrae el número total de productos disponibles en un listado desde __NEXT_DATA__"""
    try:
        script_tag = soup.find("script", {"id": "__NEXT_DATA__"})
        if script_tag and script_tag.string:
            data = json.loads(script_tag.string)
            queries = data.get("props", {}).get("pageProps", {}).get("dehydratedState", {}).get("queries", [])
            for query in queries:
                # La clave puede variar, buscamos una que contenga 'totalProducts'
                # o algo similar como 'total' dentro de una estructura relacionada con la paginación o productos
                query_state_data = query.get("state", {}).get("data", {})
                if "totalProducts" in query_state_data:
                    return query_state_data["totalProducts"]
                if "recordsFiltered" in query_state_data: # Otra posible clave
                     return query_state_data["recordsFiltered"]
                # A veces está en una estructura más anidada
                if "productSearch" in query_state_data and "recordsFiltered" in query_state_data["productSearch"]:
                    return query_state_data["productSearch"]["recordsFiltered"]

    except Exception as e:
        print(f"Advertencia: Error al obtener total de productos del listado: {e}")
    return None

def extract_products_from_listing_page(soup, sellos_tipo, categoria):
    """Extrae productos y sus URLs de detalle de una página de listado"""
    extracted_products_summary = []
    product_detail_urls = []
    
    script_tag = soup.find("script", {"id": "__NEXT_DATA__"})
    if not (script_tag and script_tag.string):
        print("  No se encontró __NEXT_DATA__ en la página de listado.")
        return extracted_products_summary, product_detail_urls

    try:
        data = json.loads(script_tag.string)
        # La estructura de 'availableProducts' puede variar. Intentar varias rutas comunes.
        products_list_json = []
        page_props = data.get("props", {}).get("pageProps", {})
        
        # Ruta 1: dehydratedState.queries[*].state.data.availableProducts
        queries = page_props.get("dehydratedState", {}).get("queries", [])
        for query_item in queries:
            state_data = query_item.get("state", {}).get("data", {})
            if "availableProducts" in state_data and isinstance(state_data["availableProducts"], list):
                products_list_json = state_data["availableProducts"]
                break
            # Ruta alternativa dentro de queries (a veces está anidado bajo 'productSearch')
            if "productSearch" in state_data and "products" in state_data["productSearch"]:
                 products_list_json = state_data["productSearch"]["products"]
                 break
        
        if not products_list_json:
             # Ruta 2: pageProps.products (menos común para listados, más para detalles)
            if "products" in page_props and isinstance(page_props["products"], list):
                products_list_json = page_props["products"]


        if not products_list_json:
            print("  No se encontró la lista de productos ('availableProducts' o similar) en __NEXT_DATA__.")
            return extracted_products_summary, product_detail_urls

        for product_json in products_list_json:
            nombre = product_json.get("nameComplete") or product_json.get("productName")
            marca = product_json.get("brand")
            sku = product_json.get("itemId") or product_json.get("productId") # itemId es más común en listados

            precio = None
            sellers = product_json.get("sellers", [])
            if sellers and isinstance(sellers, list) and len(sellers) > 0:
                # VTEX a veces tiene 'Price' y otras 'price'
                price_val = sellers[0].get("commertialOffer", {}).get("Price")
                if price_val is None:
                    price_val = sellers[0].get("commertialOffer", {}).get("price")
                precio = price_val


            url_imagen = None
            images = product_json.get("images", [])
            if images and isinstance(images, list) and len(images) > 0:
                # A veces la imagen es un dict con 'imageUrl', otras una string
                img_data = images[0]
                if isinstance(img_data, dict):
                    url_imagen = img_data.get("imageUrl")
                elif isinstance(img_data, str):
                    url_imagen = img_data

            url_producto_relativo = product_json.get("linkText") # 'linkText' suele ser el slug para la URL
            if not url_producto_relativo:
                 url_producto_relativo = product_json.get("detailUrl") # Alternativa

            url_producto_absoluto = None
            if url_producto_relativo:
                if not url_producto_relativo.startswith("/"):
                    url_producto_relativo = "/" + url_producto_relativo
                if not url_producto_relativo.endswith("/p"): # Asegurar que termine en /p si es el formato
                    # Esto es una suposición, la estructura de la URL de detalle puede variar.
                    # El formato más común es /slug/p
                    # Es mejor confiar en que el `detailUrl` o `linkText` ya esté bien formado
                    # o que la URL base del sitio + slug sea suficiente.
                    # Por ahora, asumimos que `linkText` es solo el slug.
                     url_producto_absoluto = f"https://www.unimarc.cl{url_producto_relativo}/p"


            if url_producto_absoluto: # Solo añadir si se pudo construir una URL
                product_detail_urls.append(url_producto_absoluto)
                extracted_products_summary.append({
                    "nombre": nombre,
                    "marca": marca,
                    "sku": sku,
                    "precio_listado": precio,
                    "url_imagen": url_imagen,
                    "url_producto_detalle": url_producto_absoluto,
                    "tipo_sello_filtro": sellos_tipo,
                    "categoria_listado": categoria
                })
    except json.JSONDecodeError:
        print("  Error al decodificar JSON de __NEXT_DATA__ en página de listado.")
    except Exception as e:
        print(f"  Error al procesar productos del listado desde __NEXT_DATA__: {e}")
    
    return extracted_products_summary, product_detail_urls

def scrape_product_listings(base_url_con_filtro, session):
    """Procesa todas las páginas de un listado de productos para una categoría y filtro de sello"""
    sellos_tipo = get_tipo_sello_from_url(base_url_con_filtro)
    categoria = get_categoria_from_url(base_url_con_filtro)
    
    print(f"\n--- Iniciando scraping de listado ---")
    print(f"Categoría: {categoria}, Filtro Sellos: {sellos_tipo}")
    print(f"URL base: {base_url_con_filtro}")
    
    all_products_summary_list = []
    all_product_detail_urls_list = []
    page = 1
    max_pages = 50 # Límite para evitar bucles infinitos, Unimarc suele tener 50 prod/página
    total_products_expected = None
    products_per_page_nominal = 24 # Ajustar si es necesario, Unimarc usa a veces 24, 48 o 50

    while page <= max_pages:
        # Unimarc usa 'page=' para la paginación en URLs con parámetros
        paginated_url = f"{base_url_con_filtro}&page={page}"
        print(f"  Scraping página {page}: {paginated_url}")
        
        try:
            response = session.get(paginated_url, headers=HEADERS, timeout=45)
            response.raise_for_status()
        except requests.exceptions.RequestException as e:
            print(f"  Error HTTP al acceder a página {page} del listado: {e}")
            break 

        soup = BeautifulSoup(response.text, "html.parser")
        
        # Guardar HTML de la página de listado
        ts = generar_timestamp()
        listado_html_filename = f"listado_{categoria}_{sellos_tipo}_pagina{page}_{ts}.html"
        listado_html_path = os.path.join(HTML_DIR, listado_html_filename)
        try:
            with open(listado_html_path, "w", encoding="utf-8") as f:
                f.write(soup.prettify())
        except Exception as e_write:
            print(f"  Advertencia: No se pudo guardar HTML del listado: {e_write}")

        if page == 1 and total_products_expected is None:
            total_products_expected = get_total_products_from_listing(soup)
            if total_products_expected is not None:
                print(f"  Total de productos esperados en este listado: {total_products_expected}")
                if total_products_expected == 0:
                    print("  No hay productos en este listado. Terminando.")
                    break
                # Actualizar max_pages si es posible
                # max_pages = (total_products_expected + products_per_page_nominal - 1) // products_per_page_nominal
                # print(f"  Número estimado de páginas: {max_pages}")


        products_on_this_page, urls_on_this_page = extract_products_from_listing_page(soup, sellos_tipo, categoria)
        
        if not products_on_this_page:
            print(f"  No se encontraron más productos en la página {page}. Fin del listado para {categoria} - {sellos_tipo}.")
            break

        all_products_summary_list.extend(products_on_this_page)
        all_product_detail_urls_list.extend(urls_on_this_page)
        print(f"  Extraídos {len(products_on_this_page)} productos de la página {page}.")
        
        # Condición de parada más robusta: si se extraen menos productos que el nominal y no es la primera página,
        # o si el total esperado ya se alcanzó (si se conoce).
        if len(products_on_this_page) < products_per_page_nominal and page > 1 :
             print(f"  Menos productos de lo esperado en página {page}. Asumiendo fin del listado.")
             break
        if total_products_expected is not None and len(all_products_summary_list) >= total_products_expected:
            print(f"  Se ha alcanzado el total esperado de productos ({total_products_expected}). Fin del listado.")
            break
            
        page += 1
        espera_aleatoria(1.0, 2.5) # Pausa más corta para listados

    # Guardar JSON resumen de productos para este listado específico
    if all_products_summary_list:
        ts = generar_timestamp()
        listado_json_filename = f"listado_productos_{categoria}_{sellos_tipo}_{len(all_products_summary_list)}items_{ts}.json"
        listado_json_path = os.path.join(LISTADOS_DIR, listado_json_filename)
        try:
            with open(listado_json_path, "w", encoding="utf-8") as f_json:
                json.dump(all_products_summary_list, f_json, ensure_ascii=False, indent=4)
            print(f"  Resumen de productos del listado guardado: {listado_json_path}")
        except Exception as e_write_json:
            print(f"  Advertencia: No se pudo guardar JSON del listado: {e_write_json}")

    return all_products_summary_list, list(set(all_product_detail_urls_list)) # Devolver URLs únicas

# --- Funciones de Scraping de Detalles de Producto ---

def find_key_in_json_recursive(data_item, target_key):
    """Busca recursivamente una clave en un diccionario o lista anidada."""
    if isinstance(data_item, dict):
        if target_key in data_item:
            return data_item[target_key]
        for key, value in data_item.items():
            result = find_key_in_json_recursive(value, target_key)
            if result is not None:
                return result
    elif isinstance(data_item, list):
        for item_element in data_item:
            result = find_key_in_json_recursive(item_element, target_key)
            if result is not None:
                return result
    return None

def extract_and_save_raw_json_product(soup, product_id_str, timestamp_str):
    """Extrae y guarda el JSON completo de __NEXT_DATA__ de la página de producto"""
    script_tag = soup.find("script", {"id": "__NEXT_DATA__"})
    if not (script_tag and script_tag.string):
        print("    No se encontró __NEXT_DATA__ en la página del producto o estaba vacío.")
        return None
    
    try:
        json_data = json.loads(script_tag.string)
        raw_json_filename = f"raw_json_producto_{product_id_str}_{timestamp_str}.json"
        raw_json_path = os.path.join(RAW_JSON_PRODUCTOS_DIR, raw_json_filename)
        with open(raw_json_path, "w", encoding="utf-8") as f:
            json.dump(json_data, f, ensure_ascii=False, indent=4)
        # print(f"    JSON crudo __NEXT_DATA__ guardado: {raw_json_path}") # Puede ser muy verboso
        return json_data
    except json.JSONDecodeError:
        print("    Error al decodificar JSON de __NEXT_DATA__ del producto.")
        return None
    except Exception as e:
        print(f"    Error al guardar JSON crudo del producto: {e}")
        return None

def extract_product_details_unified(next_data_json, url, product_id_str):
    """
    Extrae detalles completos (generales, nutricionales, precios) de un producto
    a partir del JSON __NEXT_DATA__ ya parseado.
    """
    if not next_data_json:
        return None

    product_details_obj = {
        "url_producto": url,
        "id_producto_scraped": product_id_str, # ID de la URL
        "fecha_extraccion": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        "datos_generales": {},
        "informacion_nutricional_completa": {},
        "detalles_precio_promocion": {}
    }
    
    page_props = next_data_json.get("props", {}).get("pageProps", {})
    
    # --- 1. Datos Generales del Producto ---
    # Intentar varias rutas para product_data
    product_data_source = None
    if "product" in page_props: # Ruta más directa
        product_data_source = page_props.get("product")
    
    if not product_data_source and "dehydratedState" in page_props: # Ruta vía dehydratedState
        queries = page_props.get("dehydratedState", {}).get("queries", [])
        for query in queries:
            # El producto puede estar bajo 'product' o anidado
            query_data = query.get("state", {}).get("data", {})
            if "product" in query_data:
                product_data_source = query_data["product"]
                break
            # A veces está en query_data.products[0].item o similar
            if "products" in query_data and isinstance(query_data["products"], list) and query_data["products"]:
                # El producto de detalle suele ser uno solo, o el primero de una lista pequeña
                # La estructura varía: a veces es el item[0] directamente, otras item[0].item
                potential_product = query_data["products"][0]
                if isinstance(potential_product, dict):
                    if "nameComplete" in potential_product: # Señal de que es el objeto producto
                         product_data_source = potential_product
                         break
                    if "item" in potential_product and isinstance(potential_product["item"], dict):
                         product_data_source = potential_product["item"]
                         break


    if product_data_source:
        product_details_obj["datos_generales"]["nombre_completo"] = product_data_source.get("nameComplete") or product_data_source.get("productName")
        product_details_obj["datos_generales"]["nombre_corto"] = product_data_source.get("name")
        product_details_obj["datos_generales"]["marca"] = product_data_source.get("brand") or product_data_source.get("brandName")
        product_details_obj["datos_generales"]["sku_principal"] = product_data_source.get("productId") or product_data_source.get("itemId") # SKU del producto
        product_details_obj["datos_generales"]["id_item"] = product_data_source.get("itemId") # SKU de la variante/item específico
        product_details_obj["datos_generales"]["id_categoria"] = product_data_source.get("categoryId")
        product_details_obj["datos_generales"]["descripcion_corta"] = product_data_source.get("description")
        
        images_list = product_data_source.get("images", [])
        if images_list and isinstance(images_list, list):
            product_details_obj["datos_generales"]["imagenes"] = [img.get("imageUrl") if isinstance(img, dict) else img for img in images_list]
            if product_details_obj["datos_generales"]["imagenes"]:
                 product_details_obj["datos_generales"]["imagen_principal_url"] = product_details_obj["datos_generales"]["imagenes"][0]

        # Especificaciones (allSpecifications y specificationGroups)
        all_specs_names = product_data_source.get("allSpecifications", [])
        spec_groups = product_data_source.get("specificationGroups", [])
        especificaciones_parsed = []
        if all_specs_names and spec_groups:
            for group in spec_groups:
                if group.get("name") in all_specs_names: # Si el grupo está en allSpecifications
                    for spec_item in group.get("specifications", []):
                        especificaciones_parsed.append({
                            "grupo": group.get("name"),
                            "nombre_especificacion": spec_item.get("name"),
                            "valores": spec_item.get("values", [])
                        })
        product_details_obj["datos_generales"]["especificaciones_producto"] = especificaciones_parsed
        
        # Sellos de Advertencia (warnings)
        warnings_list = product_data_source.get("warnings", [])
        if warnings_list and isinstance(warnings_list, list):
             product_details_obj["datos_generales"]["sellos_advertencia"] = [warn.get("name") for warn in warnings_list if isinstance(warn, dict)]
    else:
        print(f"    Advertencia: No se encontró 'product_data_source' para datos generales del producto {product_id_str}.")

    # --- 2. Información Nutricional Completa ---
    nutri_info = {}
    # Búsqueda robusta de 'nutritional_tables_sets'
    nutri_tables = find_key_in_json_recursive(next_data_json, "nutritional_tables_sets")
    if nutri_tables:
        nutri_info["tablas_nutricionales_sets"] = nutri_tables
    
    # Búsqueda de 'full_description'
    full_desc = find_key_in_json_recursive(next_data_json, "full_description")
    if full_desc:
        nutri_info["descripcion_larga_producto"] = full_desc

    # También buscar en especificaciones por "Información nutricional" como texto
    if product_data_source: # Reutilizar product_data_source si existe
        spec_groups = product_data_source.get("specificationGroups", [])
        if spec_groups:
            for group in spec_groups:
                for spec in group.get("specifications", []):
                    if spec.get("name") == "Información nutricional":
                        nutri_info["info_nutricional_texto_especificaciones"] = spec.get("values", [])
                        break
                if "info_nutricional_texto_especificaciones" in nutri_info: break
    
    product_details_obj["informacion_nutricional_completa"] = nutri_info

    # --- 3. Detalles de Precio y Promoción ---
    # La info de precio suele estar más consistentemente en pageProps.product.products[0] o similar
    price_promo_data = {}
    # product_item_for_price = None
    # if page_props.get("product") and page_props["product"].get("products"):
    #      product_item_for_price = page_props["product"]["products"][0] if page_props["product"]["products"] else None
    
    # Alternativamente, buscar 'priceDetail' o 'price' de forma recursiva,
    # pero esto puede ser menos preciso. Intentemos con la estructura más común.
    # El objeto 'product' que a veces contiene 'products' (una lista), y el primer item de esa lista es el relevante.
    product_section_pp = page_props.get("product", {})
    product_list_pp = product_section_pp.get("products", [])

    item_for_price_details = None

    if product_list_pp and isinstance(product_list_pp, list) and len(product_list_pp) > 0:
        item_for_price_details = product_list_pp[0] # Usualmente el primer (y único) item
    
    # Si no se encuentra arriba, intentar una búsqueda más genérica en dehydratedState para price y priceDetail
    if not item_for_price_details and "dehydratedState" in page_props:
        queries = page_props.get("dehydratedState", {}).get("queries", [])
        for query in queries:
            query_data = query.get("state", {}).get("data", {})
            # A veces está en query_data.products[0]
            if "products" in query_data and isinstance(query_data["products"], list) and query_data["products"]:
                potential_item = query_data["products"][0]
                if isinstance(potential_item, dict) and ("price" in potential_item or "priceDetail" in potential_item):
                    item_for_price_details = potential_item
                    break
    
    if item_for_price_details:
        # Precio base y oferta
        price_node = item_for_price_details.get("price", {})
        if price_node: # Asegurarse que price_node es un dict
            price_promo_data["precio_lista_base"] = price_node.get("listPrice")
            price_promo_data["precio_oferta_actual"] = price_node.get("price")
            price_promo_data["precio_sin_descuento_directo"] = price_node.get("priceWithoutDiscount")
            price_promo_data["ahorro_directo"] = price_node.get("saving")
            price_promo_data["precio_por_unidad_medida_oferta"] = price_node.get("ppum")
            price_promo_data["precio_por_unidad_medida_lista"] = price_node.get("ppumListPrice")

        # Detalles de promoción (priceDetail)
        price_detail_node = item_for_price_details.get("priceDetail", {})
        if price_detail_node: # Asegurarse que price_detail_node es un dict
            price_promo_data["detalle_promocion_especifica"] = {
                "tipo_promocion": price_detail_node.get("promotionType"),
                "nombre_promocion": price_detail_node.get("promotionName"),
                "id_promocion": price_detail_node.get("promotionId"),
                "codigo_tag_promocional": price_detail_node.get("promotionalTagCode"),
                "precio_lista_promo": price_detail_node.get("listPrice"), # Precio antes de esta promo específica
                "precio_descuento_promo": price_detail_node.get("discountPrice"),
                "ppum_lista_promo": price_detail_node.get("ppumListPrice"),
                "ppum_descuento_promo": price_detail_node.get("discountPpumPrice"),
                "porcentaje_descuento_promo": price_detail_node.get("discountPercentage"),
                "mensaje_promocion": price_detail_node.get("promotionMessage"),
                "items_requeridos_promo": price_detail_node.get("itemsRequiredForPromotion")
            }
            # Etiqueta promocional
            if "promotionalTag" in price_detail_node and isinstance(price_detail_node["promotionalTag"], dict):
                tag_node = price_detail_node["promotionalTag"]
                price_promo_data["detalle_promocion_especifica"]["etiqueta_visual_promo"] = {
                    "id_campania": tag_node.get("campaignId"),
                    "texto_etiqueta": tag_node.get("text"),
                    "color_texto": tag_node.get("textColor"),
                    "color_fondo": tag_node.get("color")
                }
            # Métodos de pago y membresías para la promo
            if "paymentMethod" in price_detail_node: price_promo_data["detalle_promocion_especifica"]["metodos_pago_promo"] = price_detail_node["paymentMethod"]
            if "membership" in price_detail_node: price_promo_data["detalle_promocion_especifica"]["membresias_promo"] = price_detail_node["membership"]
        
        # Promoción adicional (a veces existe un nodo 'promotion' separado)
        promo_adicional_node = item_for_price_details.get("promotion", {})
        if promo_adicional_node: # Asegurarse que es un dict
             price_promo_data["promocion_adicional_info"] = {
                "tiene_ahorro": promo_adicional_node.get("hasSavings"),
                "nombre_promo_adicional": promo_adicional_node.get("name"),
                "tipo_promo_adicional": promo_adicional_node.get("type"),
                "codigo_descripcion": promo_adicional_node.get("descriptionCode"),
                "mensaje_descripcion": promo_adicional_node.get("descriptionMessage"),
                "precio_promo_adicional": promo_adicional_node.get("price"),
                "mensaje_oferta_adicional": promo_adicional_node.get("offerMessage"),
                "ahorro_promo_adicional": promo_adicional_node.get("saving"),
                "ppum_promo_adicional": promo_adicional_node.get("ppum")
            }
    else:
        print(f"    Advertencia: No se encontró 'item_for_price_details' para precios del producto {product_id_str}.")


    product_details_obj["detalles_precio_promocion"] = price_promo_data
    
    return product_details_obj

def process_product_detail_unified(url, session):
    """Procesa una URL de producto individual para extraer y guardar toda su información"""
    print(f"  Procesando URL de producto: {url}")
    
    try:
        response = session.get(url, headers=HEADERS, timeout=30)
        response.raise_for_status()
    except requests.exceptions.RequestException as e:
        print(f"    Error HTTP al acceder a URL de producto {url}: {e}")
        return None

    soup = BeautifulSoup(response.text, "html.parser")
    
    # Extraer ID del producto de la URL para nombrar archivos
    # ej: https://www.unimarc.cl/product/aceite-vegetal-1-l/p  -> product/aceite-vegetal-1-l
    # ej: https://www.unimarc.cl/aceite-vegetal-1-l/p -> aceite-vegetal-1-l
    # Usaremos una versión sanitizada del final de la URL
    url_path_part = url.split("unimarc.cl/")[-1].replace("/p", "").replace("/", "_")
    product_id_str = "".join(c if c.isalnum() or c in ('_','-') else "_" for c in url_path_part)[-100:] # Sanitizar y truncar

    ts = generar_timestamp()
    
    # Guardar HTML del producto
    product_html_filename = f"producto_{product_id_str}_{ts}.html"
    product_html_path = os.path.join(HTML_DIR, product_html_filename)
    try:
        with open(product_html_path, "w", encoding="utf-8") as f:
            f.write(soup.prettify())
    except Exception as e_html:
         print(f"    Advertencia: No se pudo guardar HTML del producto: {e_html}")

    # Extraer y guardar JSON crudo __NEXT_DATA__
    next_data_json = extract_and_save_raw_json_product(soup, product_id_str, ts)
    if not next_data_json:
        print(f"    No se pudo obtener __NEXT_DATA__ para {url}. Saltando extracción de detalles.")
        return None # No se puede continuar sin __NEXT_DATA__

    # Extraer detalles unificados
    producto_completo = extract_product_details_unified(next_data_json, url, product_id_str)

    if not producto_completo or not (producto_completo["datos_generales"] or producto_completo["informacion_nutricional_completa"] or producto_completo["detalles_precio_promocion"]):
        print(f"    No se extrajeron suficientes datos para el producto {product_id_str} desde {url}")
        # Guardar __NEXT_DATA__ para depuración si la extracción falló pero el JSON existe
        if next_data_json:
            debug_filename = f"failed_extraction_raw_json_{product_id_str}_{ts}.json"
            debug_filepath = os.path.join(RAW_JSON_PRODUCTOS_DIR, debug_filename)
            try:
                with open(debug_filepath, "w", encoding="utf-8") as f_debug:
                    json.dump(next_data_json, f_debug, ensure_ascii=False, indent=4)
                print(f"    __NEXT_DATA__ de extracción fallida guardado en: {debug_filepath}")
            except Exception as e_write_debug:
                print(f"    Error al guardar archivo de depuración {debug_filepath}: {e_write_debug}")
        return None

    # Guardar JSON procesado completo del producto
    processed_json_filename = f"producto_procesado_{product_id_str}_{ts}.json"
    processed_json_path = os.path.join(JSON_PRODUCTOS_PROCESADOS_DIR, processed_json_filename)
    try:
        with open(processed_json_path, "w", encoding="utf-8") as f:
            json.dump(producto_completo, f, ensure_ascii=False, indent=4)
        print(f"    Detalles completos del producto guardados: {processed_json_path}")
    except Exception as e_proc_json:
        print(f"    Advertencia: No se pudo guardar JSON procesado del producto: {e_proc_json}")


    # Guardar JSON específico de información nutricional si existe
    if producto_completo.get("informacion_nutricional_completa"):
        nutri_data_to_save = {
            "url_producto": url,
            "id_producto_scraped": product_id_str,
            "nombre_producto": producto_completo.get("datos_generales", {}).get("nombre_completo"),
            "data_nutricional": producto_completo["informacion_nutricional_completa"]
        }
        nutri_filename = f"nutricional_{product_id_str}_{ts}.json"
        nutri_path = os.path.join(JSON_NUTRICIONAL_DIR, nutri_filename)
        try:
            with open(nutri_path, "w", encoding="utf-8") as f:
                json.dump(nutri_data_to_save, f, ensure_ascii=False, indent=4)
        except Exception as e_nutri_json:
            print(f"    Advertencia: No se pudo guardar JSON nutricional: {e_nutri_json}")

    # Guardar JSON específico de precios y promociones si existe
    if producto_completo.get("detalles_precio_promocion"):
        precio_data_to_save = {
            "url_producto": url,
            "id_producto_scraped": product_id_str,
            "nombre_producto": producto_completo.get("datos_generales", {}).get("nombre_completo"),
            "data_precios_promos": producto_completo["detalles_precio_promocion"]
        }
        precio_filename = f"precio_{product_id_str}_{ts}.json"
        precio_path = os.path.join(JSON_PRECIOS_DIR, precio_filename)
        try:
            with open(precio_path, "w", encoding="utf-8") as f:
                json.dump(precio_data_to_save, f, ensure_ascii=False, indent=4)
        except Exception as e_precio_json:
            print(f"    Advertencia: No se pudo guardar JSON de precios: {e_precio_json}")
            
    return producto_completo


# --- Función Principal ---
def main_supremo_scraper():
    print(f"\n{'='*80}")
    print(f"   INICIO SCRAPER UNIFICADO SUPREMO UNIMARC - {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"{'='*80}")
    
    crear_directorios()
    
    # 1. Leer URLs base de categorías
    urls_categorias_base = leer_urls_base_categorias(ARCHIVO_URLS_CATEGORIAS_BASE)
    if not urls_categorias_base:
        print("Finalizando: No se pudieron cargar URLs de categorías base.")
        return

    # 2. Generar URLs de listado con filtros de sellos
    filtros_sellos = obtener_filtros_sellos()
    urls_listado_filtradas = generar_urls_listado_con_filtros(urls_categorias_base, filtros_sellos)
    
    if not urls_listado_filtradas:
        print("Finalizando: No se generaron URLs de listado con filtros.")
        return

    # 3. Scrapear listados para obtener URLs de detalle de productos
    print(f"\n--- Iniciando Fase 1: Scraping de Listados ({len(urls_listado_filtradas)} URLs a procesar) ---")
    todas_urls_detalle_productos = []
    
    with requests.Session() as session: # Usar sesión para eficiencia
        for i, url_listado in enumerate(urls_listado_filtradas):
            print(f"\nProcesando URL de listado ({i+1}/{len(urls_listado_filtradas)})...")
            _, urls_detalle_obtenidas = scrape_product_listings(url_listado, session)
            todas_urls_detalle_productos.extend(urls_detalle_obtenidas)
            if i < len(urls_listado_filtradas) - 1:
                 espera_aleatoria(2,4) # Pausa entre diferentes listados (categoría/sello)

    urls_detalle_unicas = sorted(list(set(todas_urls_detalle_productos)))
    print(f"\n--- Fin Fase 1 ---")
    print(f"Total de URLs de detalle de productos únicas encontradas: {len(urls_detalle_unicas)}")

    # Guardar todas las URLs de detalle únicas en un archivo
    if urls_detalle_unicas:
        ts = generar_timestamp()
        urls_detalle_consolidado_filename = f"urls_detalle_productos_consolidadas_{len(urls_detalle_unicas)}_{ts}.txt"
        urls_detalle_consolidado_path = os.path.join(LISTADOS_DIR, urls_detalle_consolidado_filename)
        try:
            with open(urls_detalle_consolidado_path, "w", encoding="utf-8") as f_urls:
                for url_prod in urls_detalle_unicas:
                    f_urls.write(f"{url_prod}\n")
            print(f"Archivo de URLs de detalle consolidadas guardado: {urls_detalle_consolidado_path}")
        except Exception as e_write_consolidated_urls:
            print(f"Advertencia: No se pudo guardar archivo de URLs de detalle: {e_write_consolidated_urls}")
    else:
        print("No se encontraron URLs de detalle de productos para procesar. Finalizando.")
        return

    # 4. Scrapear detalles de cada producto
    print(f"\n--- Iniciando Fase 2: Scraping de Detalles de Productos ({len(urls_detalle_unicas)} URLs a procesar) ---")
    todos_los_productos_detallados = []
    
    with requests.Session() as session:
        for j, url_producto_detalle in enumerate(urls_detalle_unicas):
            print(f"\nProcesando detalle de producto ({j+1}/{len(urls_detalle_unicas)})...")
            producto_data = process_product_detail_unified(url_producto_detalle, session)
            if producto_data:
                todos_los_productos_detallados.append(producto_data)
            
            if j < len(urls_detalle_unicas) - 1: # No esperar después del último
                 espera_aleatoria() # Pausa entre productos individuales

    print(f"\n--- Fin Fase 2 ---")
    print(f"Total de productos con detalles extraídos: {len(todos_los_productos_detallados)}")

    # 5. Guardar el JSON consolidado de todos los productos detallados
    if todos_los_productos_detallados:
        ts = generar_timestamp()
        consolidado_filename = f"TODOS_PRODUCTOS_UNIMARC_CONSOLIDADOS_{len(todos_los_productos_detallados)}_{ts}.json"
        consolidado_path = os.path.join(BASE_DIR, consolidado_filename) # Guardar en el directorio base
        try:
            with open(consolidado_path, "w", encoding="utf-8") as f_consol:
                json.dump(todos_los_productos_detallados, f_consol, ensure_ascii=False, indent=4)
            print(f"\nArchivo JSON consolidado con todos los productos detallados guardado en: {consolidado_path}")
        except Exception as e_write_final_json:
             print(f"Error al guardar el archivo JSON consolidado final: {e_write_final_json}")
    else:
        print("\nNo se extrajeron detalles de ningún producto para el archivo consolidado.")

    print(f"\n{'='*80}")
    print(f"   FIN SCRAPER UNIFICADO SUPREMO UNIMARC - {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"   Resultados en la carpeta: {BASE_DIR}")
    print(f"{'='*80}")


if __name__ == "__main__":
    # Crear archivo dummy 'links_categorias_unimarc.txt' si no existe, para pruebas
    if not os.path.exists(ARCHIVO_URLS_CATEGORIAS_BASE):
        print(f"ADVERTENCIA: El archivo '{ARCHIVO_URLS_CATEGORIAS_BASE}' no existe.")
        print("Creando un archivo de ejemplo. Por favor, edítelo con URLs de categorías reales de Unimarc.")
        with open(ARCHIVO_URLS_CATEGORIAS_BASE, "w", encoding="utf-8") as f_dummy:
            f_dummy.write("// Formato: https://www.unimarc.cl/category/nombre-categoria/sub-categoria\n")
            f_dummy.write("https://www.unimarc.cl/category/despensa/aceites-y-vinagres\n")
            f_dummy.write("https://www.unimarc.cl/category/lacteos/leches\n")
            # f_dummy.write("https://www.unimarc.cl/category/frutas-y-verduras/frutas\n")
        print(f"Archivo de ejemplo '{ARCHIVO_URLS_CATEGORIAS_BASE}' creado. Ejecute el script de nuevo.")
    else:
        main_supremo_scraper()