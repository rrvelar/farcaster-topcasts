// app/layout.tsx
import type { Metadata, Viewport } from "next";
import "./globals.css";

// Базовый публичный URL без завершающего слэша
const APP_URL =
  process.env.NEXT_PUBLIC_APP_URL?.replace(/\/+$/, "") ||
  "https://farcaster-topcasts.vercel.app";

// Конфиг embed для Warpcast Mini App
const miniAppEmbed = {
  version: "next",
  imageUrl: `${APP_URL}/og.png`, // 1200x630 (должен существовать в /public/og.png)
  button: {
    title: "Открыть топ-касты",
    action: {
      type: "launch_miniapp",
      name: "Top Casts",
      url: `${APP_URL}/`,                // корень приложения
      splashImageUrl: `${APP_URL}/icon-512.png`, // 512x512 (public/icon-512.png)
      splashBackgroundColor: "#ffffff",
    },
  },
};

export const metadata: Metadata = {
  title: "Топ кастов — Farcaster",
  description: "Мини-приложение: топ постов Farcaster по лайкам, реплаям и рекастам.",
  applicationName: "Farcaster Top Casts",
  icons: { icon: "/favicon.ico" },
  openGraph: {
    title: "Топ кастов — Farcaster",
    description:
      "Топ-10/15 постов Farcaster по лайкам, реплаям и рекастам за выбранный период.",
    url: APP_URL,
    siteName: "Farcaster Top Casts",
    type: "website",
    images: [
      {
        url: `${APP_URL}/og.png`,
        width: 1200,
        height: 630,
        alt: "Top Casts",
      },
    ],
  },
  // Mini App мета-теги
  other: {
    // основной тег для Mini App
    "fc:miniapp": JSON.stringify(miniAppEmbed),
    // оставим fc:frame как бэкап (некоторые клиенты ещё читают его)
    "fc:frame": JSON.stringify(miniAppEmbed),
  },
};

export const viewport: Viewport = {
  themeColor: "#ffffff",
  width: "device-width",
  initialScale: 1,
  viewportFit: "cover",
};

export default function RootLayout({ children }: { children: React.ReactNode }) {
  return (
    <html lang="ru">
      <body className="min-h-dvh bg-gray-50 text-gray-900 antialiased">
        <main className="min-h-dvh">{children}</main>
      </body>
    </html>
  );
}
