'use client';

import { useState } from 'react';
import { BarChart, Bar, XAxis, YAxis, Tooltip, ResponsiveContainer, Cell } from 'recharts';
import { SimulationResult } from '@/lib/types';
import { mockSimulationResult } from '@/lib/mockData';

export default function AnalysisModule() {
  const [simulationResult, setSimulationResult] = useState<SimulationResult>(mockSimulationResult);
  const [isSimulating, setIsSimulating] = useState(false);

  const handleSimulate = async () => {
    setIsSimulating(true);
    // Simulate API call
    await new Promise(resolve => setTimeout(resolve, 2000));
    setIsSimulating(false);
    // In real app, this would fetch from API
  };

  // Create histogram data from wins array
  const histogramData = (() => {
    const bins: { [key: number]: number } = {};
    simulationResult.wins.forEach(win => {
      const bin = Math.floor(win / 5) * 5; // Round to nearest 5
      bins[bin] = (bins[bin] || 0) + 1;
    });

    return Object.entries(bins)
      .map(([wins, count]) => ({
        wins: parseInt(wins),
        count,
        label: `${wins}-${parseInt(wins) + 4}`,
      }))
      .sort((a, b) => a.wins - b.wins);
  })();

  const maxCount = Math.max(...histogramData.map(d => d.count));

  // Color bars based on wins (green for high, red for low)
  const getBarColor = (wins: number) => {
    if (wins >= 90) return '#10b981'; // green-500
    if (wins >= 80) return '#84cc16'; // lime-500
    if (wins >= 70) return '#eab308'; // yellow-500
    if (wins >= 60) return '#f97316'; // orange-500
    return '#ef4444'; // red-500
  };

  return (
    <div className="h-full flex flex-col">
      <div className="mb-6">
        <div className="flex items-center justify-between mb-4">
          <h2 className="text-2xl font-bold">Season Simulation</h2>
          <button
            onClick={handleSimulate}
            disabled={isSimulating}
            className="px-6 py-2 bg-blue-600 text-white rounded-lg hover:bg-blue-700 disabled:bg-gray-400 disabled:cursor-not-allowed transition-colors"
          >
            {isSimulating ? 'Simulating...' : 'Simulate Season'}
          </button>
        </div>
        <p className="text-gray-600">
          Run Monte Carlo simulations to predict season outcomes based on your current roster.
        </p>
      </div>

      <div className="flex-1 grid grid-cols-2 gap-6">
        {/* Bell Curve / Histogram */}
        <div className="bg-white border border-gray-300 rounded-lg p-6">
          <h3 className="text-lg font-semibold mb-4">Wins Distribution</h3>
          <ResponsiveContainer width="100%" height={400}>
            <BarChart data={histogramData}>
              <XAxis 
                dataKey="label" 
                label={{ value: 'Wins', position: 'insideBottom', offset: -5 }}
              />
              <YAxis 
                label={{ value: 'Frequency', angle: -90, position: 'insideLeft' }}
              />
              <Tooltip
                content={({ active, payload }) => {
                  if (active && payload && payload.length) {
                    const data = payload[0].payload;
                    return (
                      <div className="bg-white p-3 border border-gray-300 rounded shadow-lg">
                        <p className="font-semibold">{data.label} wins</p>
                        <p className="text-sm text-gray-600">
                          {data.count} simulations ({((data.count / simulationResult.wins.length) * 100).toFixed(1)}%)
                        </p>
                      </div>
                    );
                  }
                  return null;
                }}
              />
              <Bar dataKey="count" radius={[8, 8, 0, 0]}>
                {histogramData.map((entry, index) => (
                  <Cell key={`cell-${index}`} fill={getBarColor(entry.wins)} />
                ))}
              </Bar>
            </BarChart>
          </ResponsiveContainer>
          <div className="mt-4 text-sm text-gray-600">
            <p>Mean: {simulationResult.meanWins.toFixed(1)} wins</p>
            <p>Std Dev: {simulationResult.stdDevWins.toFixed(1)} wins</p>
          </div>
        </div>

        {/* Key Metrics */}
        <div className="space-y-6">
          {/* Playoff Probability */}
          <div className="bg-gradient-to-br from-blue-50 to-blue-100 border border-blue-200 rounded-lg p-6">
            <h3 className="text-lg font-semibold mb-2 text-blue-900">Playoff Probability</h3>
            <div className="flex items-baseline gap-2">
              <span className="text-5xl font-bold text-blue-600">
                {simulationResult.playoffProbability}%
              </span>
            </div>
            <p className="text-sm text-blue-700 mt-2">
              Based on {simulationResult.wins.length.toLocaleString()} simulations
            </p>
          </div>

          {/* Win Range */}
          <div className="bg-white border border-gray-300 rounded-lg p-6">
            <h3 className="text-lg font-semibold mb-4">Win Range</h3>
            <div className="space-y-3">
              <div>
                <div className="flex justify-between text-sm mb-1">
                  <span className="text-gray-600">10th Percentile</span>
                  <span className="font-semibold">
                    {Math.round(
                      simulationResult.wins.sort((a, b) => a - b)[
                        Math.floor(simulationResult.wins.length * 0.1)
                      ]
                    )} wins
                  </span>
                </div>
                <div className="w-full bg-gray-200 rounded-full h-2">
                  <div
                    className="bg-red-500 h-2 rounded-full"
                    style={{ width: '10%' }}
                  />
                </div>
              </div>
              <div>
                <div className="flex justify-between text-sm mb-1">
                  <span className="text-gray-600">50th Percentile (Median)</span>
                  <span className="font-semibold">
                    {Math.round(
                      simulationResult.wins.sort((a, b) => a - b)[
                        Math.floor(simulationResult.wins.length * 0.5)
                      ]
                    )} wins
                  </span>
                </div>
                <div className="w-full bg-gray-200 rounded-full h-2">
                  <div
                    className="bg-yellow-500 h-2 rounded-full"
                    style={{ width: '50%' }}
                  />
                </div>
              </div>
              <div>
                <div className="flex justify-between text-sm mb-1">
                  <span className="text-gray-600">90th Percentile</span>
                  <span className="font-semibold">
                    {Math.round(
                      simulationResult.wins.sort((a, b) => a - b)[
                        Math.floor(simulationResult.wins.length * 0.9)
                      ]
                    )} wins
                  </span>
                </div>
                <div className="w-full bg-gray-200 rounded-full h-2">
                  <div
                    className="bg-green-500 h-2 rounded-full"
                    style={{ width: '90%' }}
                  />
                </div>
              </div>
            </div>
          </div>

          {/* Key Insights */}
          <div className="bg-white border border-gray-300 rounded-lg p-6">
            <h3 className="text-lg font-semibold mb-4">Key Insights</h3>
            <ul className="space-y-2 text-sm">
              <li className="flex items-start">
                <span className="text-green-500 mr-2">✓</span>
                <span>
                  {simulationResult.playoffProbability >= 80
                    ? 'Strong playoff contender'
                    : simulationResult.playoffProbability >= 50
                    ? 'Competitive team'
                    : 'Needs improvement'}
                </span>
              </li>
              <li className="flex items-start">
                <span className="text-blue-500 mr-2">•</span>
                <span>
                  Expected wins: {simulationResult.meanWins.toFixed(1)} ± {simulationResult.stdDevWins.toFixed(1)}
                </span>
              </li>
              <li className="flex items-start">
                <span className="text-purple-500 mr-2">•</span>
                <span>
                  {((simulationResult.wins.filter(w => w >= 90).length / simulationResult.wins.length) * 100).toFixed(1)}% chance of 90+ wins
                </span>
              </li>
            </ul>
          </div>
        </div>
      </div>
    </div>
  );
}
