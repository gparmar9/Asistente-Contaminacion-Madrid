import L from 'leaflet'
import 'leaflet/dist/leaflet.css'
import { useEffect, useRef } from 'react'

import type { Estacion } from '../../api/tipos'
import { useTema } from '../../estado/hooks'
import { useColoresTema } from '../../hooks/useColoresTema'

/**
 * Mapa de estaciones (Leaflet + mapa base gris de Esri, claro u oscuro según el tema;
 * sin clave de API. CARTO se descartó porque ahora exige clave).
 *
 * Todos los marcadores van en el color de dato: el mapa muestra DÓNDE está
 * cada estación, no un juicio sobre su aire. La estación elegida en «Hoy» lleva
 * un anillo. Las estaciones sin coordenadas no se pintan: la pantalla las lista
 * aparte. Las teselas se descargan de internet; sin red, quedan solo los puntos.
 *
 * Se carga de forma perezosa (import dinámico) para no meter Leaflet en el
 * paquete principal.
 */

const TESELAS = {
  claro: 'https://server.arcgisonline.com/ArcGIS/rest/services/Canvas/World_Light_Gray_Base/MapServer/tile/{z}/{y}/{x}',
  oscuro: 'https://server.arcgisonline.com/ArcGIS/rest/services/Canvas/World_Dark_Gray_Base/MapServer/tile/{z}/{y}/{x}',
} as const

const ATRIBUCION = 'Mapa base &copy; <a href="https://www.esri.com/">Esri</a> &mdash; Esri, DeLorme, NAVTEQ'
/** El servicio Canvas de Esri llega hasta el nivel 16. */
const ZOOM_MAXIMO = 16

const CENTRO_MADRID: L.LatLngTuple = [40.4168, -3.7038]

interface Props {
  estaciones: readonly Estacion[]
  /** Estación elegida en «Hoy»: se resalta con un anillo. */
  seleccionada?: number
  onElegir: (codigo: number) => void
}

/** Contenido del tooltip construido con el DOM (textContent): los nombres vienen de la API. */
function contenidoTooltip(e: Estacion): HTMLElement {
  const raiz = document.createElement('div')
  const nombre = document.createElement('strong')
  nombre.textContent = e.nombre
  const linea = document.createElement('div')
  linea.textContent = e.distrito ? `${e.tipo} · ${e.distrito}` : e.tipo
  const sub = document.createElement('div')
  sub.className = 'mapa-tooltip-sub'
  const medidos = e.contaminantes_medidos ?? []
  sub.textContent = `${medidos.length ? medidos.join(' · ') : 'sin contaminantes objetivo'} · pulsa para ver el detalle`
  raiz.append(nombre, linea, sub)
  return raiz
}

export default function MapaEstaciones({ estaciones, seleccionada, onElegir }: Props) {
  const contenedorRef = useRef<HTMLDivElement>(null)
  const mapaRef = useRef<L.Map | null>(null)
  const teselasRef = useRef<L.TileLayer | null>(null)
  const capaRef = useRef<L.LayerGroup | null>(null)
  const encuadradoRef = useRef(false)
  const onElegirRef = useRef(onElegir)
  const { tema } = useTema()
  const colores = useColoresTema()

  useEffect(() => {
    onElegirRef.current = onElegir
  }, [onElegir])

  // El mapa se crea una vez y se destruye al desmontar.
  useEffect(() => {
    const elemento = contenedorRef.current
    if (!elemento || mapaRef.current) return
    const mapa = L.map(elemento, {
      center: CENTRO_MADRID,
      zoom: 11,
      maxZoom: ZOOM_MAXIMO,
      // La rueda hace scroll de página, no zoom: los botones +/- y el doble clic sí hacen zoom.
      scrollWheelZoom: false,
    })
    mapaRef.current = mapa
    capaRef.current = L.layerGroup().addTo(mapa)
    return () => {
      mapa.remove()
      mapaRef.current = null
      capaRef.current = null
      teselasRef.current = null
      encuadradoRef.current = false
    }
  }, [])

  // Teselas claras u oscuras según el tema.
  useEffect(() => {
    const mapa = mapaRef.current
    if (!mapa) return
    teselasRef.current?.remove()
    teselasRef.current = L.tileLayer(TESELAS[tema], { attribution: ATRIBUCION, maxZoom: ZOOM_MAXIMO }).addTo(mapa)
  }, [tema])

  // Marcadores: se redibujan al cambiar la lista, la selección o los colores del tema.
  useEffect(() => {
    const mapa = mapaRef.current
    const capa = capaRef.current
    if (!mapa || !capa) return
    capa.clearLayers()
    const posiciones: L.LatLngTuple[] = []

    for (const e of estaciones) {
      if (e.latitud == null || e.longitud == null) continue
      const posicion: L.LatLngTuple = [e.latitud, e.longitud]
      posiciones.push(posicion)
      const elegida = e.codigo_corto === seleccionada

      if (elegida) {
        L.circleMarker(posicion, { radius: 14, color: colores.dato, weight: 2, fill: false, interactive: false }).addTo(capa)
      }
      const marcador = L.circleMarker(posicion, {
        radius: elegida ? 8 : 7,
        color: colores.superficie, // anillo en el color de la superficie, como los puntos de las gráficas
        weight: 2,
        fillColor: colores.dato,
        fillOpacity: 1,
      })
      marcador.bindTooltip(contenidoTooltip(e), { direction: 'top', offset: [0, -8], className: 'mapa-tooltip' })
      marcador.on('click', () => onElegirRef.current(e.codigo_corto))
      marcador.addTo(capa)
    }

    // Encuadre inicial a todas las estaciones; después se respeta lo que haya movido la persona.
    if (posiciones.length > 0 && !encuadradoRef.current) {
      mapa.fitBounds(L.latLngBounds(posiciones), { padding: [28, 28], maxZoom: 13 })
      encuadradoRef.current = true
    }
  }, [estaciones, seleccionada, colores])

  return <div ref={contenedorRef} className="mapa-estaciones" role="region" aria-label="Mapa de las estaciones de control" />
}
