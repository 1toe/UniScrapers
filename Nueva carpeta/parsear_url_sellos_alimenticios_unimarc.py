import os

def leer_urls_base(archivo_links):
    """Lee las URLs base desde el archivo de links_categorias_unimarc"""
    urls_base = []
    try:
        with open(archivo_links, 'r', encoding='utf-8') as file:
            for linea in file:
                linea = linea.strip()
                if linea and not linea.startswith('//'):
                    urls_base.append(linea)
        print(f"Se cargaron {len(urls_base)} URLs base desde {archivo_links}")
        return urls_base
    except Exception as e:
        print(f"Error al leer el archivo de URLs: {e}")
        return []

def obtener_filtros_sellos():
    """Obtiene los filtros de sellos alimenticios"""
    return [
        "?warningStamps=sin-sellos",
        "?warningStamps=un-sello",
        "?warningStamps=dos-sellos"
    ]

def generar_urls_con_filtros(urls_base, filtros):
    """Genera nuevas URLs combinando cada URL base con cada filtro"""
    urls_combinadas = []
    for url in urls_base:
        for filtro in filtros:
            # Verificar si la URL ya tiene parámetros
            if '?' in url:
                nueva_url = f"{url}&{filtro.lstrip('?')}"
            else:
                nueva_url = f"{url}{filtro}"
            urls_combinadas.append(nueva_url)
    return urls_combinadas

def guardar_urls_combinadas(urls_combinadas, archivo_salida):
    """Guarda las URLs combinadas en un archivo de salida"""
    try:
        with open(archivo_salida, 'w', encoding='utf-8') as file:
            for url in urls_combinadas:
                file.write(f"{url}\n")
        print(f"Se han guardado {len(urls_combinadas)} URLs en {archivo_salida}")
    except Exception as e:
        print(f"Error al guardar las URLs combinadas: {e}")

def main():
    directorio_actual = os.path.dirname(os.path.abspath(__file__))
    archivo_links = os.path.join(directorio_actual, "links_categorias_unimarc.txt")
    archivo_salida = os.path.join(directorio_actual, "urls_con_filtros.txt")
    
    # Leer URLs base y filtros
    urls_base = leer_urls_base(archivo_links)
    filtros = obtener_filtros_sellos()
    
    if not urls_base:
        print("No se pudieron cargar las URLs base.")
        return
    
    # Generar URLs combinadas
    urls_combinadas = generar_urls_con_filtros(urls_base, filtros)
    print(f"Se generaron {len(urls_combinadas)} URLs combinadas.")
    
    # Guardar URLs combinadas
    guardar_urls_combinadas(urls_combinadas, archivo_salida)

if __name__ == "__main__":
    main()