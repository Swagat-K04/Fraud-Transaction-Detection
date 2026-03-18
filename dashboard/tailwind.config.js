/** @type {import('tailwindcss').Config} */
export default {
  content: ['./index.html', './src/**/*.{js,jsx}'],
  theme: {
    extend: {
      fontFamily: {
        mono: ['"JetBrains Mono"', 'monospace'],
        sans: ['"Syne"', 'sans-serif'],
      },
      colors: {
        fraud:    { DEFAULT: '#E24B4A', bg: '#FCEBEB', border: '#F09595' },
        safe:     { DEFAULT: '#3B6D11', bg: '#EAF3DE', border: '#97C459' },
        amber:    { DEFAULT: '#BA7517', bg: '#FAEEDA', border: '#EF9F27' },
        critical: '#E24B4A',
        high:     '#BA7517',
        medium:   '#185FA5',
        low:      '#3B6D11',
      },
    },
  },
  plugins: [],
}
