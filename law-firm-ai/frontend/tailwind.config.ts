import type { Config } from "tailwindcss";

const config: Config = {
  content: ["./app/**/*.{ts,tsx}", "./components/**/*.{ts,tsx}"],
  theme: {
    extend: {
      colors: {
        surface: {
          0: "#0a0a0a",
          1: "#111111",
          2: "#1a1a1a",
          3: "#222222",
          border: "#2a2a2a",
        },
        accent: {
          DEFAULT: "#7c3aed",
          light: "#a78bfa",
          blue: "#3b82f6",
        },
      },
    },
  },
  plugins: [],
};

export default config;
