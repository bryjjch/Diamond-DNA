import { Routes, Route, Navigate } from "react-router-dom";
import { Header } from "@/components/Header";
import { Tabs } from "@/components/Tabs";
import { ClusterBrowser } from "@/pages/ClusterBrowser";
import { SimilarPlayers } from "@/pages/SimilarPlayers";

export default function App() {
  return (
    <>
      <Header />
      <Tabs />
      <main className="mx-auto max-w-6xl px-6 pb-12 pt-5">
        <Routes>
          <Route path="/" element={<ClusterBrowser />} />
          <Route path="/similar" element={<SimilarPlayers />} />
          <Route path="*" element={<Navigate to="/" replace />} />
        </Routes>
      </main>
    </>
  );
}
