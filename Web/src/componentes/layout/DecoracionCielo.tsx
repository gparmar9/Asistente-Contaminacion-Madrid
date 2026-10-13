/**
 * Decoración de la paleta «Cielo»: perfil discreto de Madrid con cielo abierto
 * (Cuatro Torres, cúpula de la Almudena, Torres Kio, Puerta de Alcalá y
 * Torrespaña) y líneas suaves de aire. Solo trazo, sin relleno, en el color de
 * acento; se oculta en la paleta original (ver paleta-cielo.css).
 */
export function DecoracionCielo() {
  return (
    <svg className="decoracion-cielo" viewBox="0 0 640 170" preserveAspectRatio="xMaxYMax meet" aria-hidden="true" focusable="false">
      <g fill="none" stroke="currentColor" strokeWidth="1.5" strokeLinecap="round" strokeLinejoin="round">
        {/* Líneas de aire */}
        <path className="aire" d="M20 34 C 110 10, 190 56, 290 32 S 470 8, 600 30" />
        <path className="aire" d="M60 58 C 150 36, 230 78, 330 54 S 500 32, 620 52" opacity="0.7" />
        <path className="aire" d="M140 80 C 220 62, 290 96, 380 76 S 540 58, 630 74" opacity="0.45" />

        {/* Suelo */}
        <path d="M8 160 H632" />

        {/* Cuatro Torres */}
        <path d="M60 160 V74 h16 V160" />
        <path d="M84 160 V62 h18 V160" />
        <path d="M110 160 V70 h14 V160 M117 70 V54" />
        <path d="M132 160 V66 l16 -6 V160" />

        {/* Almudena: cúpula y linterna */}
        <path d="M190 160 V124 a24 24 0 0 1 48 0 V160 M214 100 V92 M208 100 h12" />

        {/* Torres Kio, inclinadas una hacia la otra */}
        <path d="M282 160 L296 96 h34 L316 160" />
        <path d="M352 160 L338 96 h34 L386 160" />

        {/* Puerta de Alcalá */}
        <path d="M424 160 V118 h84 V160 M424 118 h84 M434 160 V134 a9 9 0 0 1 18 0 V160 M457 160 V130 a11 11 0 0 1 22 0 V160 M484 160 V134 a9 9 0 0 1 18 0 V160 M452 118 V108 h28 V118" />

        {/* Torrespaña */}
        <path d="M572 160 V60 M566 92 h12 M572 60 V44" />
        <ellipse cx="572" cy="84" rx="7" ry="3" />
      </g>
    </svg>
  )
}
