export default function RootLayout({ children }: { children: React.ReactNode }) {
  // Пока используем заглушки-картинки с placehold.co. После деплоя заменишь на свой домен.
  const embed = {
    version: "next",
    imageUrl: "https://placehold.co/1200x630/png",
    button: {
      title: "Открыть топ-касты",
      action: {
        type: "launch_miniapp",
        name: "Top Casts",
        url: "https://YOUR_DOMAIN/",             // ← замени после деплоя
        splashImageUrl: "https://placehold.co/512x512/png",
        splashBackgroundColor: "#ffffff"
      }
    }
  };

  return (
    <html lang="ru">
      <head>
        <meta name="fc:frame" content={JSON.stringify(embed)} />
        <meta name="viewport" content="width=device-width, initial-scale=1" />
        <title>Top Casts</title>
      </head>
      <body style={{ margin: 0, fontFamily: "ui-sans-serif, system-ui", background: "#f9fafb" }}>
        {children}
      </body>
    </html>
  );
}

