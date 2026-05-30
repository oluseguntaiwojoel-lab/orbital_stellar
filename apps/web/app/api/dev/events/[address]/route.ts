import { NextResponse } from "next/server";

export const dynamic = "force-dynamic";
export const runtime = "nodejs";

export async function GET(
  req: Request,
  { params }: { params: Promise<{ address: string }> }
) {
  const { address } = await params;

  const encoder = new TextEncoder();

  const stream = new ReadableStream({
    start(controller) {
      let closed = false;

      const close = () => {
        if (closed) return;
        closed = true;
        clearInterval(tick);
        try {
          controller.close();
        } catch {}
      };

      let count = 0;
      const tick = setInterval(() => {
        if (closed) return;
        count += 1;
        const payload = {
          type: "payment.received",
          to: address,
          from: `GDEMO${Math.random().toString(36).slice(2,8).toUpperCase()}`,
          amount: (Math.random() * 10).toFixed(2),
          asset: "XLM",
          timestamp: new Date().toISOString(),
          raw: { demo: true, seq: count },
        };
        try {
          controller.enqueue(
            encoder.encode(`data: ${JSON.stringify(payload)}\n\n`)
          );
        } catch {
          close();
        }
      }, 1000);

      // heartbeat
      const hb = setInterval(() => {
        if (closed) return;
        try {
          controller.enqueue(encoder.encode(`: heartbeat\n\n`));
        } catch {
          close();
        }
      }, 10_000);

      req.signal.addEventListener("abort", () => {
        clearInterval(hb);
        close();
      });
    },
  });

  return new Response(stream, {
    headers: {
      "Content-Type": "text/event-stream",
      "Cache-Control": "no-cache, no-transform",
      Connection: "keep-alive",
      "X-Accel-Buffering": "no",
    },
  });
}
