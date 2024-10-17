import asyncio
from selenium import webdriver
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.common.by import By
from concurrent.futures import ThreadPoolExecutor
import time
import pandas as pd

# Define la función para obtener las estaciones meteorológicas
def get_estacion_meteorologica(url, municipio):
    tusa = 0
    while True and tusa <= 3:
        try:
            time.sleep(1)
            # Configura el driver de Selenium
            driver = webdriver.Chrome()
            driver.get(url)

            # Maximizar ventana
            #driver.maximize_window()

            # Intentar seleccionar Cookies
            print(f"Intentando seleccionar cookies para {municipio}")
            select_cookies = WebDriverWait(driver, 40).until(EC.presence_of_element_located(('xpath', '//*[@id="sp_message_iframe_1165301"]')))
            driver.switch_to.frame(select_cookies)
            time.sleep(2)
            tries = 0
            while True and tries <= 3:
                try:
                    aceptar_cookies = driver.find_element(By.CSS_SELECTOR, "#notice > div.message-component.message-row.cta-buttons-container > div.message-component.message-column.cta-button-column.reject-column > button")
                    aceptar_cookies.click()
                    time.sleep(1)
                    print(f"Cookies aceptadas en {municipio}")
                    break
                except Exception as e:
                    print(f"No se pudieron aceptar las cookies en {municipio}: {e}")
                    tries += 1
                    time.sleep(1)

            driver.switch_to.default_content()

            # Esperar para cargar la página completamente
            time.sleep(3)

            # Buscar y hacer clic en la estación meteorológica
            driver.find_element(By.CSS_SELECTOR, "#inner-content > div.region-content-top > lib-city-header > div:nth-child(1) > div > div > a.station-name").click()
            time.sleep(4)

            # Obtener el nombre de la estación
            nombre_estacion = driver.find_element(By.CSS_SELECTOR, "#inner-content > div.region-content-top > app-dashboard-header > div.dashboard__header.small-12.ng-star-inserted > div > div.heading > h1").text.split(" - ")[1]
            print(f"Estación meteorológica para {municipio}: {nombre_estacion}")

            driver.quit()
            return municipio, nombre_estacion
        except Exception as e:
            print(f"Error al obtener estación meteorológica para {municipio}: {e}")
            if driver:
                driver.quit()
            tusa += 1
            
# Función asíncrona que utilizará el executor para lanzar tareas en paralelo
async def fetch_all_municipios(urls, municipios):
    estaciones_meteorologicas = []
    
    # Crear un pool de threads
    with ThreadPoolExecutor(max_workers=5) as executor:
        # Lanzar tareas para obtener las estaciones meteorológicas
        loop = asyncio.get_event_loop()
        tasks = [
            loop.run_in_executor(executor, get_estacion_meteorologica, url, municipio)
            for url, municipio in zip(urls, municipios)
        ]
        # Recoger los resultados conforme se completan las tareas
        for result in await asyncio.gather(*tasks):
            estaciones_meteorologicas.append(result)

    return estaciones_meteorologicas

# Función principal asíncrona
async def main():
    municipios = ['La Acebeda', 'Ajalvir', 'Alameda del Valle', 'El Álamo', 'Alcalá de Henares', 'Alcobendas', 'Alcorcón', 'Aldea del Fresno', 'Algete', 'Alpedrete', 'Ambite', 'Anchuelo', 'Aranjuez', 'Arganda del Rey', 'Arroyomolinos', 'El Atazar', 'Batres', 'Becerril de la Sierra', 'Belmonte de Tajo', 'El Berrueco', 'Berzosa del Lozoya', 'Boadilla del Monte', 'El Boalo', 'Braojos', 'Brea de Tajo', 'Brunete', 'Buitrago del Lozoya', 'Bustarviejo', 'Cabanillas de la Sierra', 'Cabrera Lane', 'Cadalso de los Vidrios', 'Camarma de Esteruelas', 'Campo Real', 'Canencia', 'Carabaña', 'Casarrubuelos', 'Cenicientos', 'Cercedilla', 'Cervera de Buitrago', 'Chapinería', 'Chinchón', 'Ciempozuelos', 'Cobeña', 'Collado Mediano', 'Collado Villalba', 'Colmenar del Arroyo', 'Colmenar de Oreja', 'Colmenarejo', 'Colmenar Viejo', 'Corpa', 'Coslada', 'Cubas de la Sagra', 'Daganzo de Arriba', 'Real Monasterio de San Lorenzo de El Escorial', 'Estremera', 'Fresnedillas de la Oliva', 'Fresno de Torote', 'Fuenlabrada', 'Fuente el Saz de Jarama', 'Fuentidueña de Tajo', 'Galapagar', 'Garganta de los Montes', 'Gargantilla del Lozoya y Pinilla de Buitrago', 'Gascones', 'Getafe', 'Griñón', 'Guadalix de la Sierra', 'Guadarrama', 'La Hiruela', 'Horcajo de la Sierra-Aoslos', 'Horcajuelo de la Sierra', 'Hoyo de Manzanares', 'Humanes de Madrid', 'Leganes', 'Loeches', 'Lozoya', 'Lozoyuela-Navas-Sieteiglesias', 'Madarcos', 'Madrid', 'Majadahonda', 'Manzanares el Real', 'Meco', 'Mejorada del Campo', 'Miraflores de la Sierra', 'El Molar', 'Molinos Los', 'Montejo de la Sierra', 'Moraleja de Enmedio', 'Moralzarzal', 'Morata de Tajuña', 'Móstoles', 'Navacerrada', 'Navalafuente', 'Navalagamella', 'Navalcarnero', 'Navarredonda y San Mamés', 'Navas del Rey', 'Nuevo Baztán', 'Olmeda de las Fuentes', 'Orusco de Tajuña', 'Paracuellos de Jarama', 'Parla', 'Patones', 'Pedrezuela', 'Pelayos de la Presa', 'Perales de Tajuña', 'Pezuela de las Torres', 'Pinilla del Valle', 'Pinto', 'Piñuécar-Gandullas', 'Pozuelo de Alarcón', 'Pozuelo del Rey', 'Prádena del Rincón', 'Puebla de la Sierra', 'Manjirón', 'Quijorna', 'Rascafría', 'Redueña', 'Ribatejada', 'Rivas-Vaciamadrid', 'Robledillo de la Jara', 'Robledo de Chavela', 'Robregordo', 'Las Rozas de Madrid', 'Rozas de Puerto Real', 'San Agustín del Guadalix', 'San Fernando de Henares', 'San Lorenzo de El Escorial', 'San Martín de la Vega', 'San Martín de Valdeiglesias', 'San Sebastián de los Reyes', 'Santa María de la Alameda', 'Santorcaz', 'Los Santos de la Humosa', 'La Serna del Monte', 'Serranillos del Valle', 'Sevilla la Nueva', 'Somosierra', 'Soto del Real', 'Talamanca de Jarama', 'Tielmes', 'Titulcia', 'Torrejón de Ardoz', 'Torrejón de la Calzada', 'Torrejón de Velasco', 'Torrelaguna', 'Torrelodones', 'Torremocha de Jarama', 'Torres de la Alameda', 'Tres Cantos', 'Valdaracete', 'Valdeavero', 'Valdelaguna', 'Valdemanco', 'Valdemaqueda', 'Valdemorillo', 'Valdemoro', 'Valdeolmos-Alalpardo', 'Valdepiélagos', 'Valdetorres de Jarama', 'Valdilecha', 'Valverde de Alcalá', 'Velilla de San Antonio', 'El Vellón', 'Venturada', 'Villaconejos', 'Villa del Prado', 'Villalbilla', 'Villamanrique de Tajo', 'Villamanta', 'Villamantilla', 'Villanueva de la Cañada', 'Villanueva del Pardillo', 'Villanueva de Perales', 'Villar del Olmo', 'Villarejo de Salvanés', 'Villaviciosa de Odón', 'Villavieja del Lozoya', 'Zarzalejo']
    #municipios = ['La Acebeda', 'Ajalvir', 'Alameda del Valle', 'El Álamo', 'Alcalá de Henares']  # Prueba con unos pocos municipios
    url_base = "https://www.wunderground.com/weather/es/{}"
    urls_to_do = [url_base.format(municipio.replace(" ", "%20")) for municipio in municipios]
    
    # Ejecutar la función para obtener las estaciones meteorológicas
    estaciones_meteorologicas = await fetch_all_municipios(urls_to_do, municipios)

    # Crear un dataframe con los resultados
    df = pd.DataFrame(estaciones_meteorologicas, columns=["Municipio", "Estación Meteorológica"])
    print(df)

if __name__ == "__main__":
    asyncio.run(main())
