'use client';

import { useState, useMemo } from 'react';
import { PlayerCard } from '@/lib/types';
import { mockPlayers } from '@/lib/mockData';
import { ScatterChart, Scatter, XAxis, YAxis, Tooltip, ResponsiveContainer, Cell } from 'recharts';

interface DiscoveryModuleProps {
  onPlayerSelect?: (player: PlayerCard) => void;
}

export default function DiscoveryModule({ onPlayerSelect }: DiscoveryModuleProps) {
  const [searchQuery, setSearchQuery] = useState('');
  const [selectedPlayerId, setSelectedPlayerId] = useState<string | null>(null);
  const [hoveredPlayerId, setHoveredPlayerId] = useState<string | null>(null);

  const filteredPlayers = useMemo(() => {
    if (!searchQuery.trim()) return mockPlayers;
    const query = searchQuery.toLowerCase();
    return mockPlayers.filter(p => 
      p.name.toLowerCase().includes(query) ||
      p.position.toLowerCase().includes(query)
    );
  }, [searchQuery]);

  const selectedPlayer = useMemo(() => {
    if (!selectedPlayerId) return null;
    return mockPlayers.find(p => p.id === selectedPlayerId) || null;
  }, [selectedPlayerId]);

  const nearestNeighbors = useMemo(() => {
    if (!selectedPlayer || !selectedPlayer.nearestNeighbors) return [];
    return selectedPlayer.nearestNeighbors
      .map(id => mockPlayers.find(p => p.id === id))
      .filter((p): p is PlayerCard => p !== undefined);
  }, [selectedPlayer]);

  const chartData = filteredPlayers.map(player => ({
    x: player.x,
    y: player.y,
    id: player.id,
    name: player.name,
    position: player.position,
    isSelected: player.id === selectedPlayerId,
    isNeighbor: selectedPlayerId && selectedPlayer?.nearestNeighbors?.includes(player.id),
  }));

  const handlePointClick = (data: any) => {
    if (!data) return;
    const playerId = data.id || data.payload?.id;
    if (playerId) {
      const player = mockPlayers.find(p => p.id === playerId);
      if (player) {
        setSelectedPlayerId(player.id);
        onPlayerSelect?.(player);
      }
    }
  };

  const getColor = (point: any) => {
    if (point.isSelected) return '#ef4444'; // red-500
    if (point.isNeighbor) return '#3b82f6'; // blue-500
    return '#94a3b8'; // slate-400
  };

  const CustomTooltip = ({ active, payload }: any) => {
    if (active && payload && payload.length) {
      const data = payload[0].payload;
      return (
        <div className="bg-white p-3 border border-gray-300 rounded shadow-lg">
          <p className="font-semibold">{data.name}</p>
          <p className="text-sm text-gray-600">{data.position}</p>
        </div>
      );
    }
    return null;
  };

  return (
    <div className="h-full flex flex-col">
      <div className="mb-6">
        <h2 className="text-2xl font-bold mb-4">Player Discovery</h2>
        <div className="relative">
          <input
            type="text"
            placeholder="Search for a player..."
            value={searchQuery}
            onChange={(e) => setSearchQuery(e.target.value)}
            className="w-full px-4 py-2 border border-gray-300 rounded-lg focus:outline-none focus:ring-2 focus:ring-blue-500"
          />
        </div>
      </div>

      <div className="flex-1 relative">
        <ResponsiveContainer width="100%" height="100%">
          <ScatterChart
            margin={{ top: 20, right: 20, bottom: 20, left: 20 }}
          >
            <XAxis 
              type="number" 
              dataKey="x" 
              name="Dimension 1"
              domain={[0, 1]}
              tick={false}
              axisLine={false}
            />
            <YAxis 
              type="number" 
              dataKey="y" 
              name="Dimension 2"
              domain={[0, 1]}
              tick={false}
              axisLine={false}
            />
            <Tooltip content={<CustomTooltip />} />
            <Scatter
              data={chartData}
              fill="#8884d8"
              onClick={(data) => handlePointClick(data)}
              onMouseEnter={(data) => setHoveredPlayerId(data?.id || null)}
              onMouseLeave={() => setHoveredPlayerId(null)}
            >
              {chartData.map((entry, index) => (
                <Cell
                  key={`cell-${index}`}
                  fill={getColor(entry)}
                  r={entry.isSelected ? 8 : entry.isNeighbor ? 6 : 4}
                />
              ))}
            </Scatter>
          </ScatterChart>
        </ResponsiveContainer>

        {/* Draw lines to nearest neighbors */}
        {selectedPlayer && nearestNeighbors.length > 0 && (
          <svg
            className="absolute inset-0 pointer-events-none"
            style={{ width: '100%', height: '100%' }}
          >
            {nearestNeighbors.map((neighbor) => {
              // Calculate positions (this is simplified - you'd need actual chart coordinates)
              const selectedX = selectedPlayer.x;
              const selectedY = selectedPlayer.y;
              const neighborX = neighbor.x;
              const neighborY = neighbor.y;
              
              // Convert normalized coordinates to SVG coordinates
              // This is approximate - Recharts doesn't expose exact pixel coordinates easily
              const svgWidth = 100;
              const svgHeight = 100;
              
              return (
                <line
                  key={neighbor.id}
                  x1={`${selectedX * svgWidth}%`}
                  y1={`${selectedY * svgHeight}%`}
                  x2={`${neighborX * svgWidth}%`}
                  y2={`${neighborY * svgHeight}%`}
                  stroke="#3b82f6"
                  strokeWidth="1"
                  strokeDasharray="4,4"
                  opacity={0.5}
                />
              );
            })}
          </svg>
        )}
      </div>

      {selectedPlayer && (
        <div className="mt-6 p-4 bg-gray-50 rounded-lg">
          <h3 className="font-semibold text-lg mb-2">{selectedPlayer.name} - {selectedPlayer.position}</h3>
          <div className="grid grid-cols-2 gap-4 text-sm">
            <div>
              <p><span className="font-medium">Salary:</span> ${selectedPlayer.salary.toFixed(1)}M</p>
              <p><span className="font-medium">Health Risk:</span> 
                <span className={`ml-2 px-2 py-1 rounded text-xs ${
                  selectedPlayer.healthRisk === 'low' ? 'bg-green-100 text-green-800' :
                  selectedPlayer.healthRisk === 'medium' ? 'bg-yellow-100 text-yellow-800' :
                  'bg-red-100 text-red-800'
                }`}>
                  {selectedPlayer.healthRisk}
                </span>
              </p>
            </div>
            <div>
              <p><span className="font-medium">Performance Variance:</span>
                <span className={`ml-2 px-2 py-1 rounded text-xs ${
                  selectedPlayer.performanceVariance === 'low' ? 'bg-green-100 text-green-800' :
                  selectedPlayer.performanceVariance === 'medium' ? 'bg-yellow-100 text-yellow-800' :
                  'bg-red-100 text-red-800'
                }`}>
                  {selectedPlayer.performanceVariance}
                </span>
              </p>
            </div>
          </div>
          {nearestNeighbors.length > 0 && (
            <div className="mt-4">
              <p className="font-medium mb-2">5 Nearest Similar Players:</p>
              <div className="flex flex-wrap gap-2">
                {nearestNeighbors.map(neighbor => (
                  <span
                    key={neighbor.id}
                    className="px-2 py-1 bg-blue-100 text-blue-800 rounded text-xs cursor-pointer hover:bg-blue-200"
                    onClick={() => {
                      setSelectedPlayerId(neighbor.id);
                      onPlayerSelect?.(neighbor);
                    }}
                  >
                    {neighbor.name}
                  </span>
                ))}
              </div>
            </div>
          )}
        </div>
      )}
    </div>
  );
}
