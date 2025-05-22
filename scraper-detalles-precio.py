import requests
from bs4 import BeautifulSoup
import json
import os
from datetime import datetime
import time
import random

'''
Script especializado en la extracción de información detallada de precios y promociones
de productos de Unimarc.

Funcionalidades principales:
1. Lee URLs de productos desde un archivo de texto
2. Para cada URL, extrae específicamente:
   - Precios normales y de oferta
   - Información de descuentos y promociones
   - Mensajes promocionales
   - Tipos de promoción y porcentajes de descuento
   - Método de pago asociado a promociones
   - Categorías de membresía que acceden a ofertas especiales
3. Guarda los resultados en formato JSON estructurado por producto
   - Un archivo consolidado con todos los productos
   - Archivos individuales por producto

Este script es útil para análisis de estrategias de precios, monitoreo de ofertas
y comprensión de los esquemas promocionales utilizados por Unimarc.
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

def extract_price_details(soup, url):
    """Extrae detalles específicos de precio y promociones de un producto"""
    price_details = {
        "url": url,
        "fecha_extraccion": datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    }
    
    print(f"Extrayendo datos de precios desde {url}")
    script_tag = soup.find("script", {"id": "__NEXT_DATA__"})

    if not script_tag or not script_tag.string:
        print("No se encontró el JSON __NEXT_DATA__ o está vacío.")
        return None
    
    try:
        data = json.loads(script_tag.string)
        
        # Extraer información básica del producto para contexto
        product_name = None
        product_id = None
        
        # Buscar en diferentes rutas del JSON
        # 1. Buscar en props.pageProps.product.products[0]
        try:
            product = data.get("props", {}).get("pageProps", {}).get("product", {}).get("products", [])[0]
            if product:
                # Extraer nombre e ID
                if "item" in product:
                    product_name = product["item"].get("nameComplete") or product["item"].get("name")
                    product_id = product["item"].get("sku") or product["item"].get("itemId")
                
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
                
                # Extraer información de promoción adicional
                if "promotion" in product:
                    promotion = product.get("promotion", {})
                    price_details["promocion_adicional"] = {
                        "tiene_ahorro": promotion.get("hasSavings"),
                        "nombre": promotion.get("name"),
                        "tipo": promotion.get("type"),
                        "codigo_descripcion": promotion.get("descriptionCode"),
                        "mensaje_descripcion": promotion.get("descriptionMessage"),
                        "precio": promotion.get("price"),
                        "mensaje_oferta": promotion.get("offerMessage"),
                        "ahorro": promotion.get("saving"),
                        "precio_unitario": promotion.get("ppum")
                    }
        except (IndexError, KeyError, TypeError):
            pass
        
        # 2. Buscar en la estructura dehydratedState si no se encontraron datos
        if not product_name or not product_id:
            try:
                queries = data.get("props", {}).get("pageProps", {}).get("dehydratedState", {}).get("queries", [])
                for query in queries:
                    state_data = query.get("state", {}).get("data", {})
                    products = state_data.get("products", [])
                    if products and len(products) > 0:
                        first_product = products[0]
                        
                        # Intentar extraer información básica
                        if "item" in first_product:
                            product_name = product_name or first_product["item"].get("nameComplete") or first_product["item"].get("name")
                            product_id = product_id or first_product["item"].get("sku") or first_product["item"].get("itemId")
                        
                        # Si no se encontró información de precio anteriormente, intentar aquí
                        if "precio_normal" not in price_details and "price" in first_product:
                            price_data = first_product.get("price", {})
                            price_details["precio_normal"] = price_data.get("listPrice")
                            price_details["precio_oferta"] = price_data.get("price")
                            price_details["precio_sin_descuento"] = price_data.get("priceWithoutDiscount")
                            price_details["ahorro"] = price_data.get("saving")
                            price_details["precio_unitario"] = price_data.get("ppum")
                            price_details["precio_unitario_lista"] = price_data.get("ppumListPrice")
                        
                        # Si no se encontraron detalles de promoción, intentar aquí
                        if "detalles_precio" not in price_details and "priceDetail" in first_product:
                            promo_data = first_product.get("priceDetail", {})
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
            except (IndexError, KeyError, TypeError):
                pass
        
        # Añadir información básica del producto
        price_details["nombre_producto"] = product_name
        price_details["id_producto"] = product_id
        
        if not product_name and not product_id:
            print("No se pudo encontrar información básica del producto.")
        
        if "precio_normal" not in price_details and "detalles_precio" not in price_details:
            print("No se encontró información de precios en las estructuras esperadas.")
            return None
        
        return price_details
        
    except json.JSONDecodeError:
        print("Error al decodificar el JSON de __NEXT_DATA__.")
        return None
    except Exception as e:
        print(f"Error al extraer detalles de precio: {e}")
        return None

def save_price_detail_json(price_details, product_id):
    """Guarda los detalles de precio de un producto en un archivo JSON individual"""
    try:
        # Crear directorio para guardar los JSONs individuales
        json_price_folder = "Precios_Productos_Unimarc"
        os.makedirs(json_price_folder, exist_ok=True)
        
        # Usar timestamp para evitar sobreescribir archivos con el mismo ID
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        
        # Nombre del archivo con el ID del producto
        json_filename = f"precios_{product_id}_{timestamp}.json"
        json_path = os.path.join(json_price_folder, json_filename)
        
        # Guardar los detalles en formato JSON
        with open(json_path, "w", encoding="utf-8") as f:
            json.dump(price_details, f, ensure_ascii=False, indent=4)
        
        print(f"Detalles de precio guardados en archivo individual: {json_path}")
        return True
    except Exception as e:
        print(f"Error al guardar archivo JSON individual para producto {product_id}: {e}")
        return False

def scrape_price_details(urls_list):
    """Procesa cada URL de producto y extrae sus detalles de precio"""
    all_price_details = []
    total_urls = len(urls_list)
    
    for idx, url in enumerate(urls_list, 1):
        try:
            print(f"\n[{idx}/{total_urls}] Procesando URL: {url}")
            response = requests.get(url, headers=headers)
            
            if response.status_code != 200:
                print(f"Error al acceder a la URL {url}: {response.status_code}")
                continue

            soup = BeautifulSoup(response.text, "html.parser")
            
            # Extraer detalles de precio
            price_details = extract_price_details(soup, url)
            
            if price_details:
                # Obtener ID del producto para el nombre del archivo
                product_id = price_details.get("id_producto", url.split("/")[-2] if url.endswith("/p") else url.split("/")[-1])
                
                # Guardar en archivo JSON individual
                save_price_detail_json(price_details, product_id)
                
                # Añadir al listado completo
                all_price_details.append(price_details)
                print(f"Detalles de precio extraídos para: {price_details.get('nombre_producto', 'Nombre no disponible')}")
            else:
                print(f"No se pudieron extraer detalles de precio para la URL: {url}")
            
            # Espera aleatoria para evitar bloqueos
            wait_time = random.uniform(1.0, 2.5)
            print(f"Esperando {wait_time:.2f} segundos antes de la siguiente solicitud...")
            time.sleep(wait_time)
            
        except Exception as e:
            print(f"Error al procesar la URL {url}: {e}")
    
    return all_price_details

def main():
    # Crear directorios necesarios
    json_folder = "Precios_Consolidados_Unimarc"
    os.makedirs(json_folder, exist_ok=True)
    
    # Definir archivo de entrada
    archivo_urls = "Detail URLs Unimarc/urls-productos.txt"
    
    # Leer URLs de productos
    urls_productos = leer_urls_desde_archivo(archivo_urls)
    
    if not urls_productos:
        print("No se pudieron cargar las URLs de productos. Verifique el archivo.")
        return
    
    # Extraer detalles de precios de cada producto
    print(f"\n{'='*50}")
    print(f"Iniciando extracción de detalles de precios para {len(urls_productos)} productos")
    print(f"{'='*50}")
    
    detalles_precios = scrape_price_details(urls_productos)
    
    # Guardar resultados consolidados en formato JSON
    if detalles_precios:
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        json_filename = f"precios_productos_unimarc_{len(detalles_precios)}_productos_{timestamp}.json"
        json_path = os.path.join(json_folder, json_filename)
        
        with open(json_path, "w", encoding="utf-8") as f_json:
            json.dump(detalles_precios, f_json, ensure_ascii=False, indent=4)
        
        print(f"\nTotal de detalles de precios extraídos: {len(detalles_precios)}")
        print(f"Archivo JSON consolidado guardado como: {json_path}")
        print(f"Además, cada producto ha sido guardado en su propio archivo JSON en la carpeta Precios_Productos_Unimarc")
    else:
        print("\nNo se lograron extraer detalles de precios de productos.")

if __name__ == "__main__":
    main()
