import { Link } from 'react-router-dom'

export default function NoEncontrada() {
  return (
    <div className="apilado">
      <h1>Página no encontrada</h1>
      <p className="texto-2">La dirección no corresponde a ninguna pantalla.</p>
      <p>
        <Link to="/">Volver a Hoy</Link>
      </p>
    </div>
  )
}
