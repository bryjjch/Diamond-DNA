import { Routes, Route, Navigate } from "react-router-dom";
import { AppShell } from "@/components/AppShell";
import { ClusterBrowser } from "@/pages/ClusterBrowser";
import { SimilarPlayers } from "@/pages/SimilarPlayers";

export default function App() {
  return (
    <AppShell>
      <Routes>
        <Route path="/" element={<ClusterBrowser />} />
        <Route path="/similar" element={<SimilarPlayers />} />
        <Route path="*" element={<Navigate to="/" replace />} />
      </Routes>
    </AppShell>
  );
}
