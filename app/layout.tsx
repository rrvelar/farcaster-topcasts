// app/layout.tsx
import type { Metadata, Viewport } from "next";
import "./globals.css";

const APP_URL =
  process.env.NEXT_PUBLIC_APP_URL?.replace(/\/+$/, "") || "https://YOUR_DOMAIN";

const fcFrame = {
  version: "next",
  imageUrl: `${APP_URL}/og.png`, // поставь свою картинку (можно оставить плейсхолдер)
  button: {
    title: "Открыть топ-касты",
    action: {
      type: "launch_miniapp",
      name: "Top Casts",
      url: `${APP_URL}/`,
      splashImageUrl: `${APP_URL}/icon.png`,
      splashBackgroundColor: "#ffffff",
    },
  },
};

export const metadata: Metadata = {
  title: "Топ кастов — Farcaster",
  description:
    "Мини-приложение: топ постов Farcaster по лайкам, реплаям и рекастам.",
  applicationName: "Farcaster Top Casts",
  icons: { icon: "/favicon.ico" },
  openGraph: {
    title: "Топ кастов — Farcaster",
    description:
      "Топ-10/15 постов Farcaster по лайкам, реплаям и рекастам за выбранный период.",
    url: APP_URL,
    siteName: "Farcaster Top Casts",
    type: "website",
  },
  // Вставляем fc:frame через metadata.other
  other: {
    "fc:frame": JSON.stringify(fcFrame),
  },
};

export const viewport: Viewport = {
  themeColor: "#ffffff",
  width: "device-width",
  initialScale: 1,
  viewportFit: "cover",
};

export default function RootLayout({
  children,
}: {
  children: React.ReactNode;
}) {
  return (
    <html lang="ru">
      <body className="min-h-dvh bg-gray-50 text-gray-900 antialiased">
        <main className="min-h-dvh">{children}</main>
      </body>
    </html>
  );
}
