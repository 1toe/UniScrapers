import requests
from bs4 import BeautifulSoup
import json
import os
from datetime import datetime
import time
import random

'''
SCRIPT UNIFICADO DE SCRAPING UNIMARC
Este script combina todas las funcionalidades de scraping para productos Unimarc:
1. Extracción de listados de productos desde URLs con filtros de sellos
2. Extracción de información detallada de productos individuales
3. Extracción de tablas nutricionales
4. Extracción de detalles de precios y promociones

Todo en un único flujo de proceso automatizado.
'''

# Configuración global
HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/123.0.0.0 Safari/537.36"
}

# Directorios para guardar resultados
BASE_DIR = "Resultados_Unimarc"
HTML_DIR = os.path.join(BASE_DIR, "HTML")
JSON_DIR = os.path.join(BASE_DIR, "JSON")
RAW_JSON_DIR = os.path.join(BASE_DIR, "RAW_JSON")
PRECIOS_DIR = os.path.join(BASE_DIR, "Precios")
NUTRI_DIR = os.path.join(BASE_DIR, "Nutricional")
LISTADO_DIR = os.path.join(BASE_DIR, "Listados")

def crear_directorios():
    """Crea la estructura de directorios necesaria para guardar resultados"""
    directorios = [
        HTML_DIR, JSON_DIR, RAW_JSON_DIR, PRECIOS_DIR,
        NUTRI_DIR, LISTADO_DIR
    ]
    for directorio in directorios:
        os.makedirs(directorio, exist_ok=True)
        print(f"Directorio creado/verificado: {directorio}")

def generar_timestamp():
    """Genera un timestamp único para nombrar archivos"""
    return datetime.now().strftime("%Y%m%d_%H%M%S")

def espera_aleatoria(min_seg=1.0, max_seg=3.0):
    """Espera un tiempo aleatorio entre solicitudes para evitar bloqueos"""
    wait_time = random.uniform(min_seg, max_seg)
    print(f"Esperando {wait_time:.2f} segundos antes de la siguiente solicitud...")
    time.sleep(wait_time)

def leer_urls_desde_archivo(archivo):
    """Lee las URLs desde un archivo de texto"""
    urls = []
    try:
        with open(archivo, 'r', encoding='utf-8') as file:
            for linea in file:
                linea = linea.strip()
                if linea and not linea.startswith('//'):
                    urls.append(linea)
        
        if urls:
            print(f"URLs cargadas exitosamente desde {archivo}: {len(urls)} URLs")
        else:
            print(f"No se encontraron URLs válidas en {archivo}")
    except FileNotFoundError:
        print(f"El archivo {archivo} no existe.")
        return None
    except Exception as e:
        print(f"Error al leer el archivo de URLs: {e}")
        return None
    
    return urls

def get_tipo_sello_from_url(url):
    """Extrae el tipo de sello de la URL"""
    if "warningStamps=sin-sellos" in url:
        return "sin-sellos"
    elif "warningStamps=un-sello" in url:
        return "un-sello"
    elif "warningStamps=dos-sellos" in url:
        return "dos-sellos"
    else:
        return "desconocido"

def get_categoria_from_url(url):
    """Extrae la categoría de la URL"""
    try:
        categoria = url.split("/category/")[1].split("?")[0].replace("/", "_")
        return categoria
    except:
        return "categoria_desconocida"

def get_total_products(soup):
    """Extrae el número total de productos disponibles"""
    try:
        script_tag = soup.find("script", {"id": "__NEXT_DATA__"})
        if script_tag and script_tag.string:
            data = json.loads(script_tag.string)
            queries = data.get("props", {}).get("pageProps", {}).get("dehydratedState", {}).get("queries", [])
            for query in queries:
                if "totalProducts" in query.get("state", {}).get("data", {}):
                    return query["state"]["data"]["totalProducts"]
    except Exception as e:
        print(f"Error al obtener total de productos: {e}")
    return None

def extract_products_from_page(soup, sellos_tipo, categoria):
    """Extrae productos de una página de listado"""
    extracted_products = []
    product_detail_urls = []
    print("Extrayendo datos de productos desde __NEXT_DATA__...")
    script_tag = soup.find("script", {"id": "__NEXT_DATA__"})

    if script_tag:
        json_data_string = script_tag.string
        if json_data_string:
            try:
                data = json.loads(json_data_string)
                products_list_json = data.get("props", {}).get("pageProps", {}).get("dehydratedState", {}).get("queries", [])
                
                found_products_array = None
                for query_item in products_list_json:
                    if query_item.get("state", {}).get("data", {}).get("availableProducts"):
                        found_products_array = query_item["state"]["data"]["availableProducts"]
                        break
                
                if found_products_array:
                    for product_json in found_products_array:
                        nombre = product_json.get("nameComplete")
                        marca = product_json.get("brand")
                        sku = product_json.get("itemId")

                        precio = None
                        sellers = product_json.get("sellers", [])
                        if sellers and len(sellers) > 0:
                            precio = sellers[0].get("price")

                        url_imagen = None
                        images = product_json.get("images", [])
                        if images and len(images) > 0:
                            url_imagen = images[0]
                        
                        url_producto_relativo = product_json.get("detailUrl")
                        url_producto_absoluto = None
                        if url_producto_relativo:
                            url_producto_absoluto = f"https://www.unimarc.cl{url_producto_relativo}"
                            # Agregar a la lista de URLs de detalle
                            if url_producto_absoluto:
                                product_detail_urls.append(url_producto_absoluto)

                        extracted_products.append({
                            "nombre": nombre,
                            "marca": marca,
                            "sku": sku,
                            "precio": precio,
                            "url_imagen": url_imagen,
                            "url_producto": url_producto_absoluto,
                            "sellos_advertencia": sellos_tipo,
                            "categoria": categoria
                        })
                else:
                    print("No se encontró la clave 'availableProducts' en la ruta esperada.")

            except json.JSONDecodeError:
                print("Error al decodificar el JSON de __NEXT_DATA__.")
            except (KeyError, IndexError, TypeError) as e:
                print(f"Error al navegar la estructura JSON de __NEXT_DATA__: {e}")
        else:
            print("La etiqueta <script id='__NEXT_DATA__'> no tiene contenido.")
    else:
        print("No se encontró la etiqueta <script id='__NEXT_DATA__'.")

    return extracted_products, product_detail_urls

def scrape_product_listings(base_url):
    """Procesa todas las páginas de un listado de productos"""
    sellos_tipo = get_tipo_sello_from_url(base_url)
    categoria = get_categoria_from_url(base_url)
    
    print(f"\n{'='*50}")
    print(f"Procesando listado: {categoria} - {sellos_tipo}")
    print(f"{'='*50}")
    
    all_products = []
    all_product_detail_urls = []
    page = 1
    total_products = None
    
    while True:
        url = f"{base_url}&page={page}"
        print(f"\nRealizando solicitud a URL - Página {page}: {url}")
        response = requests.get(url, headers=HEADERS)
        
        if response.status_code != 200:
            print(f"Error al acceder a la página {page}: {response.status_code}")
            break

        soup = BeautifulSoup(response.text, "html.parser")
        
        if page == 1:
            total_products = get_total_products(soup)
            if total_products:
                print(f"Total de productos encontrados para {sellos_tipo}: {total_products}")
                expected_pages = (total_products + 49) // 50
                print(f"Número esperado de páginas: {expected_pages}")

        # Guardar HTML
        timestamp = generar_timestamp()
        html_filename = f"listado_{categoria}_{sellos_tipo}_page{page}_{timestamp}.html"
        html_path = os.path.join(HTML_DIR, html_filename)
        
        with open(html_path, "w", encoding="utf-8") as f:
            f.write(soup.prettify())
        print(f"HTML de página {page} guardado como: {html_path}")

        # Extraer productos y URLs
        products_in_page, urls_in_page = extract_products_from_page(soup, sellos_tipo, categoria)
        if not products_in_page:
            print(f"No se encontraron más productos en la página {page}")
            break

        all_products.extend(products_in_page)
        all_product_detail_urls.extend(urls_in_page)
        print(f"Extraídos {len(products_in_page)} productos de la página {page}")
        
        # Si hay menos de 50 productos, probablemente sea la última página
        if len(products_in_page) < 50:
            break
            
        page += 1
        espera_aleatoria()

    # Guardar JSON de productos extraídos
    if all_products:
        timestamp = generar_timestamp()
        json_filename = f"productos_{categoria}_{sellos_tipo}_{len(all_products)}_productos_{timestamp}.json"
        json_path = os.path.join(LISTADO_DIR, json_filename)
        
        with open(json_path, "w", encoding="utf-8") as f_json:
            json.dump(all_products, f_json, ensure_ascii=False, indent=4)
        print(f"\nTotal de productos guardados para {sellos_tipo}: {len(all_products)}")
        print(f"Archivo JSON guardado como: {json_path}")

    # Guardar URLs de detalle
    if all_product_detail_urls:
        timestamp = generar_timestamp()
        urls_filename = f"detalle_urls_{categoria}_{sellos_tipo}_{len(all_product_detail_urls)}_productos_{timestamp}.txt"
        urls_path = os.path.join(LISTADO_DIR, urls_filename)
        
        with open(urls_path, "w", encoding="utf-8") as f_urls:
            for url_prod in all_product_detail_urls:
                if url_prod:
                    f_urls.write(f"{url_prod}\n")
        print(f"\nURLs de detalle guardadas: {len(all_product_detail_urls)}")
        print(f"Archivo de URLs guardado como: {urls_path}")

    return all_products, all_product_detail_urls

def extract_and_save_raw_json(soup, product_id):
    """Extrae y guarda el JSON completo de __NEXT_DATA__"""
    script_tag = soup.find("script", {"id": "__NEXT_DATA__"})
    
    if not script_tag or not script_tag.string:
        print("No se encontró el JSON __NEXT_DATA__ o está vacío.")
        return None
    
    try:
        # Formatear el JSON para mejor legibilidad
        json_data = json.loads(script_tag.string)
        json_formatted = json.dumps(json_data, ensure_ascii=False, indent=4)
        
        # Guardar el JSON completo
        timestamp = generar_timestamp()
        json_filename = f"raw_json_producto_{product_id}_{timestamp}.json"
        json_path = os.path.join(RAW_JSON_DIR, json_filename)
        
        with open(json_path, "w", encoding="utf-8") as f:
            f.write(json_formatted)
        
        print(f"JSON completo guardado: {json_path}")
        return json_data
    except json.JSONDecodeError:
        print("Error al decodificar el JSON de __NEXT_DATA__.")
        return None
    except Exception as e:
        print(f"Error al guardar el JSON completo: {e}")
        return None

def extract_product_details(soup, url, product_id):
    """Extrae detalles completos de un producto individual"""
    print(f"Extrayendo detalles del producto: {url}")
    
    # Inicializar objeto de resultado
    product_details = {
        "url": url,
        "id_producto": product_id,
        "fecha_extraccion": datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    }
    
    script_tag = soup.find("script", {"id": "__NEXT_DATA__"})
    if not script_tag or not script_tag.string:
        print("No se encontró el JSON __NEXT_DATA__ o está vacío.")
        return None
    
    try:
        data = json.loads(script_tag.string)
        
        # 1. Extraer datos básicos del producto
        # Buscar en diferentes ubicaciones posibles del JSON
        product_data = None
        pageProps = data.get("props", {}).get("pageProps", {})
        
        if "product" in pageProps:
            product_data = pageProps.get("product")
        elif "dehydratedState" in pageProps:
            queries = pageProps.get("dehydratedState", {}).get("queries", [])
            for query in queries:
                if query.get("state", {}).get("data", {}).get("product"):
                    product_data = query["state"]["data"]["product"]
                    break
        
        if product_data:
            # Datos básicos
            product_details["nombre"] = product_data.get("nameComplete") or product_data.get("name")
            product_details["nombre_corto"] = product_data.get("name")
            product_details["marca"] = product_data.get("brand")
            product_details["sku"] = product_data.get("productId") or product_data.get("itemId")
            product_details["categoria"] = product_data.get("categoryId")
            product_details["descripcion"] = product_data.get("description")
            
            # Extraer imágenes
            images = product_data.get("images", [])
            if images:
                product_details["imagenes"] = images
                product_details["imagen_principal"] = images[0] if images else None
            
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
            
            # Extraer sellos de advertencia
            sellos = []
            if "warnings" in product_data:
                warnings = product_data.get("warnings", [])
                for warning in warnings:
                    sellos.append(warning.get("name"))
            product_details["sellos_advertencia"] = sellos
        
        # 2. Extraer información nutricional
        nutritional_info = {}
        
        # Buscar en diferentes ubicaciones del JSON
        # Primero, buscar directamente en producto
        if product_data and "nutritional_tables_sets" in product_data:
            nutritional_info["nutritional_tables_sets"] = product_data.get("nutritional_tables_sets")
        
        # Si no se encontró, buscar en dehydratedState
        if not nutritional_info.get("nutritional_tables_sets"):
            if "dehydratedState" in pageProps:
                queries = pageProps.get("dehydratedState", {}).get("queries", [])
                for query in queries:
                    state_data = query.get("state", {}).get("data", {})
                    
                    # Buscar en data.response
                    if "data" in state_data and "response" in state_data.get("data", {}):
                        response_data = state_data["data"]["response"]
                        if "nutritional_tables_sets" in response_data:
                            nutritional_info["nutritional_tables_sets"] = response_data["nutritional_tables_sets"]
                            break
                    
                    # Buscar directamente en data
                    if "nutritional_tables_sets" in state_data:
                        nutritional_info["nutritional_tables_sets"] = state_data["nutritional_tables_sets"]
                        break
                    
                    # Buscar en productos[0]
                    products = state_data.get("products", [])
                    if products and len(products) > 0:
                        first_product = products[0]
                        if "nutritional_tables_sets" in first_product:
                            nutritional_info["nutritional_tables_sets"] = first_product["nutritional_tables_sets"]
                            break
                        if "item" in first_product and "nutritional_tables_sets" in first_product["item"]:
                            nutritional_info["nutritional_tables_sets"] = first_product["item"]["nutritional_tables_sets"]
                            break
        
        if nutritional_info:
            product_details["informacion_nutricional"] = nutritional_info
        
        # 3. Extraer información de precios y promociones
        price_details = {}
        
        # Buscar en props.pageProps.product.products[0]
        try:
            product = pageProps.get("product", {}).get("products", [])[0]
            if product:
                # Extraer información de precio
                if "price" in product:
                    price_data = product.get("price", {})
                    price_details["precio_normal"] = price_data.get("listPrice")
                    price_details["precio_oferta"] = price_data.get("price")
                    price_details["precio_sin_descuento"] = price_data.get("priceWithoutDiscount")
                    price_details["ahorro"] = price_data.get("saving")
                    price_details["precio_unitario"] = price_data.get("ppum")
                    price_details["precio_unitario_lista"] = price_data.get("ppumListPrice")
                
                # Extraer datos de promoción
                if "priceDetail" in product:
                    promo_data = product.get("priceDetail", {})
                    price_details["detalles_precio"] = {
                        "tipo_promocion": promo_data.get("promotionType"),
                        "nombre_promocion": promo_data.get("promotionName"),
                        "id_promocion": promo_data.get("promotionId"),
                        "codigo_tag_promocional": promo_data.get("promotionalTagCode"),
                        "precio_lista": promo_data.get("listPrice"),
                        "precio_unitario_lista": promo_data.get("ppumListPrice"),
                        "precio_descuento": promo_data.get("discountPrice"),
                        "precio_unitario_descuento": promo_data.get("discountPpumPrice"),
                        "porcentaje_descuento": promo_data.get("discountPercentage"),
                        "mensaje_promocion": promo_data.get("promotionMessage"),
                        "items_requeridos": promo_data.get("itemsRequiredForPromotion")
                    }
                    
                    # Extraer etiqueta promocional si existe
                    if "promotionalTag" in promo_data:
                        promotional_tag = promo_data.get("promotionalTag", {})
                        price_details["etiqueta_promocional"] = {
                            "id_campania": promotional_tag.get("campaignId"),
                            "texto": promotional_tag.get("text"),
                            "color_texto": promotional_tag.get("textColor"),
                            "color_fondo": promotional_tag.get("color")
                        }
                    
                    # Extraer métodos de pago para promoción
                    if "paymentMethod" in promo_data:
                        payment_methods = promo_data.get("paymentMethod", [])
                        if payment_methods:
                            price_details["metodos_pago"] = payment_methods
                    
                    # Extraer membresías para promoción
                    if "membership" in promo_data:
                        memberships = promo_data.get("membership", [])
                        if memberships:
                            price_details["membresias"] = memberships
        except (IndexError, KeyError, TypeError):
            pass
        
        # Si no se encontraron datos de precio, buscar en dehydratedState
        if not price_details:
            try:
                queries = pageProps.get("dehydratedState", {}).get("queries", [])
                for query in queries:
                    state_data = query.get("state", {}).get("data", {})
                    products = state_data.get("products", [])
                    if products and len(products) > 0:
                        first_product = products[0]
                        
                        # Extraer información de precio
                        if "price" in first_product:
                            price_data = first_product.get("price", {})
                            price_details["precio_normal"] = price_data.get("listPrice")
                            price_details["precio_oferta"] = price_data.get("price")
                            price_details["precio_sin_descuento"] = price_data.get("priceWithoutDiscount")
                            price_details["ahorro"] = price_data.get("saving")
                            price_details["precio_unitario"] = price_data.get("ppum")
                            price_details["precio_unitario_lista"] = price_data.get("ppumListPrice")
                        
                        # Extraer datos de promoción
                        if "priceDetail" in first_product:
                            promo_data = first_product.get("priceDetail", {})
                            price_details["detalles_precio"] = {
                                "tipo_promocion": promo_data.get("promotionType"),
                                "nombre_promocion": promo_data.get("promotionName"),
                                "id_promocion": promo_data.get("promotionId"),
                                "precio_lista": promo_data.get("listPrice"),
                                "porcentaje_descuento": promo_data.get("discountPercentage"),
                                "mensaje_promocion": promo_data.get("promotionMessage")
                            }
            except (IndexError, KeyError, TypeError):
                pass
        
        if price_details:
            product_details["detalles_precio"] = price_details
        
        return product_details
        
    except json.JSONDecodeError:
        print("Error al decodificar el JSON de __NEXT_DATA__.")
        return None
    except Exception as e:
        print(f"Error al extraer detalles del producto: {e}")
        return None

def process_product_detail(url, session=None):
    """Procesa una URL de producto individual para extraer toda su información"""
    if session is None:
        session = requests.Session()
    
    try:
        print(f"\nProcesando producto: {url}")
        response = session.get(url, headers=HEADERS)
        
        if response.status_code != 200:
            print(f"Error al acceder a la URL {url}: {response.status_code}")
            return None

        soup = BeautifulSoup(response.text, "html.parser")
        
        # Extraer ID del producto de la URL
        product_id = url.split("/")[-2] if url.endswith("/p") else url.split("/")[-1].split("?")[0]
        
        # Guardar el HTML
        timestamp = generar_timestamp()
        html_filename = f"producto_{product_id}_{timestamp}.html"
        html_path = os.path.join(HTML_DIR, html_filename)
        
        with open(html_path, "w", encoding="utf-8") as f:
            f.write(soup.prettify())
        
        # Extraer y guardar el JSON completo de __NEXT_DATA__
        extract_and_save_raw_json(soup, product_id)
        
        # Extraer detalles completos del producto
        product_details = extract_product_details(soup, url, product_id)
        
        if product_details:
            # Guardar detalles del producto
            timestamp = generar_timestamp()
            json_filename = f"producto_{product_id}_{timestamp}.json"
            json_path = os.path.join(JSON_DIR, json_filename)
            
            with open(json_path, "w", encoding="utf-8") as f:
                json.dump(product_details, f, ensure_ascii=False, indent=4)
            print(f"Detalles del producto guardados: {json_path}")
            
            # Si hay información nutricional, guardar en archivo separado
            if "informacion_nutricional" in product_details and product_details["informacion_nutricional"]:
                nutri_filename = f"nutricional_{product_id}_{timestamp}.json"
                nutri_path = os.path.join(NUTRI_DIR, nutri_filename)
                
                nutri_data = {
                    "url_producto": url,
                    "id_producto": product_id,
                    "nombre_producto": product_details.get("nombre"),
                    "tabla_nutricional": product_details["informacion_nutricional"]
                }
                
                with open(nutri_path, "w", encoding="utf-8") as f:
                    json.dump(nutri_data, f, ensure_ascii=False, indent=4)
                print(f"Información nutricional guardada: {nutri_path}")
            
            # Si hay información de precios, guardar en archivo separado
            if "detalles_precio" in product_details and product_details["detalles_precio"]:
                precio_filename = f"precio_{product_id}_{timestamp}.json"
                precio_path = os.path.join(PRECIOS_DIR, precio_filename)
                
                precio_data = {
                    "url_producto": url,
                    "id_producto": product_id,
                    "nombre_producto": product_details.get("nombre"),
                    "detalles_precio": product_details["detalles_precio"]
                }
                
                with open(precio_path, "w", encoding="utf-8") as f:
                    json.dump(precio_data, f, ensure_ascii=False, indent=4)
                print(f"Información de precios guardada: {precio_path}")
            
            return product_details
        
        return None
        
    except Exception as e:
        print(f"Error al procesar la URL {url}: {e}")
        return None

def main():
    print(f"\n{'='*70}")
    print(f"   INICIANDO SCRAPING COMPLETO DE UNIMARC - {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"{'='*70}")
    
    # Crear directorios para resultados
    crear_directorios()
    
    # Leer URLs desde archivo
    urls_file = "urls_con_filtros.txt"
    urls_list = leer_urls_desde_archivo(urls_file)
    
    if not urls_list:
        print("No se pudieron cargar URLs válidas. Verifique el archivo.")
        return
    
    # Recolectar todas las URLs de productos
    all_detail_urls = []
    all_products_listado = []
    
    # PARTE 1: Procesar todas las URLs de listados y extraer productos
    for url_index, url in enumerate(urls_list, 1):
        print(f"\nProcesando URL de listado {url_index}/{len(urls_list)}")
        products, detail_urls = scrape_product_listings(url)
        all_products_listado.extend(products)
        all_detail_urls.extend(detail_urls)
    
    # Eliminar duplicados en URLs de detalle
    unique_detail_urls = list(set(all_detail_urls))
    print(f"\nTotal de productos en listados: {len(all_products_listado)}")
    print(f"Total de URLs únicas de detalle: {len(unique_detail_urls)}")
    
    # Guardar todas las URLs de detalle en un archivo consolidado
    timestamp = generar_timestamp()
    combined_urls_file = f"urls_productos_consolidado_{timestamp}.txt"
    combined_urls_path = os.path.join(LISTADO_DIR, combined_urls_file)
    
    with open(combined_urls_path, "w", encoding="utf-8") as f_urls:
        for url in unique_detail_urls:
            f_urls.write(f"{url}\n")
    print(f"Archivo consolidado de URLs guardado: {combined_urls_path}")
    
    # PARTE 2: Procesar cada URL de detalle para extraer información completa
    print(f"\n{'='*70}")
    print(f"   PROCESANDO URLS DE PRODUCTOS INDIVIDUALES")
    print(f"{'='*70}")
    
    all_product_details = []
    with requests.Session() as session:
        for index, url in enumerate(unique_detail_urls, 1):
            print(f"\n[{index}/{len(unique_detail_urls)}] Procesando producto")
            product_data = process_product_detail(url, session)
            if product_data:
                all_product_details.append(product_data)
            espera_aleatoria(1.0, 2.0)
    
    # Guardar todos los resultados en un archivo JSON consolidado
    if all_product_details:
        timestamp = generar_timestamp()
        final_json_file = f"resultados_completos_{len(all_product_details)}_productos_{timestamp}.json"
        final_json_path = os.path.join(BASE_DIR, final_json_file)
        
        with open(final_json_path, "w", encoding="utf-8") as f:
            json.dump(all_product_details, f, ensure_ascii=False, indent=4)
        print(f"\nTodos los datos consolidados guardados en: {final_json_path}")
    
    print(f"\n{'='*70}")
    print(f"   PROCESO COMPLETADO - {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"   Total de productos procesados: {len(all_product_details)}")
    print(f"{'='*70}")

if __name__ == "__main__":
    main()
