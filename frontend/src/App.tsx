import { Routes, Route, Navigate } from "react-router-dom";
import { AppShell } from "@/components/AppShell";
import { Home } from "@/pages/Home";
import { PlayerDetail } from "@/pages/PlayerDetail";
import { Accuracy } from "@/pages/Accuracy";

export default function App() {
  return (
    <AppShell>
      <Routes>
        <Route path="/" element={<Home />} />
        <Route path="/player/:playerId" element={<PlayerDetail />} />
        <Route path="/accuracy" element={<Accuracy />} />
        <Route path="*" element={<Navigate to="/" replace />} />
      </Routes>
    </AppShell>
  );
}
