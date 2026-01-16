'use client';

import { useState } from 'react';
import { RadarChart, PolarGrid, PolarAngleAxis, PolarRadiusAxis, Radar, ResponsiveContainer, Legend } from 'recharts';
import { TradeComparison, Player } from '@/lib/types';
import { mockTradeComparison, mockPlayers } from '@/lib/mockData';

export default function OptimizationModule() {
  const [currentComparison, setCurrentComparison] = useState<TradeComparison>(mockTradeComparison);
  const [selectedPosition, setSelectedPosition] = useState<string>('all');

  // Prepare radar chart data
  const radarData = (() => {
    const current = currentComparison.currentPlayer;
    const recommended = currentComparison.recommendedPlayer;
    
    const attributes = current.position === 'P' 
      ? ['ERA', 'Strikeouts', 'Wins']
      : ['BA', 'HR', 'RBI', 'SB', 'OBP', 'SLG'];

    return attributes.map(attr => {
      const key = attr.toLowerCase().replace(/\s+/g, '');
      let currentVal = 0;
      let recommendedVal = 0;

      if (current.position === 'P') {
        if (key === 'era') {
          currentVal = current.attributes.era || 0;
          recommendedVal = recommended.attributes.era || 0;
          // Invert ERA for radar (lower is better)
          currentVal = 6 - currentVal;
          recommendedVal = 6 - recommendedVal;
        } else if (key === 'strikeouts') {
          currentVal = (current.attributes.strikeouts || 0) / 3;
          recommendedVal = (recommended.attributes.strikeouts || 0) / 3;
        } else if (key === 'wins') {
          currentVal = (current.attributes.wins || 0) * 2;
          recommendedVal = (recommended.attributes.wins || 0) * 2;
        }
      } else {
        if (key === 'ba') {
          currentVal = current.attributes.battingAverage * 1000;
          recommendedVal = recommended.attributes.battingAverage * 1000;
        } else if (key === 'hr') {
          currentVal = current.attributes.homeRuns * 2;
          recommendedVal = recommended.attributes.homeRuns * 2;
        } else if (key === 'rbi') {
          currentVal = current.attributes.rbis;
          recommendedVal = recommended.attributes.rbis;
        } else if (key === 'sb') {
          currentVal = current.attributes.stolenBases * 3;
          recommendedVal = recommended.attributes.stolenBases * 3;
        } else if (key === 'obp') {
          currentVal = current.attributes.onBasePercentage * 1000;
          recommendedVal = recommended.attributes.onBasePercentage * 1000;
        } else if (key === 'slg') {
          currentVal = current.attributes.sluggingPercentage * 1000;
          recommendedVal = recommended.attributes.sluggingPercentage * 1000;
        }
      }

      return {
        attribute: attr,
        current: Math.round(currentVal),
        recommended: Math.round(recommendedVal),
      };
    });
  })();

  const formatDelta = (value: number, isPositive: boolean) => {
    const sign = isPositive ? '+' : '';
    const color = isPositive ? 'text-green-600' : 'text-red-600';
    return (
      <span className={color}>
        {sign}{value.toFixed(1)}
      </span>
    );
  };

  const ArrowIcon = ({ isUp }: { isUp: boolean }) => (
    <svg
      className={`w-5 h-5 ${isUp ? 'text-green-600' : 'text-red-600'}`}
      fill="none"
      stroke="currentColor"
      viewBox="0 0 24 24"
    >
      {isUp ? (
        <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M5 15l7-7 7 7" />
      ) : (
        <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M19 9l-7 7-7-7" />
      )}
    </svg>
  );

  return (
    <div className="h-full flex flex-col">
      <div className="mb-6">
        <h2 className="text-2xl font-bold mb-2">Trade Analyzer</h2>
        <p className="text-gray-600">
          Compare current players with recommended replacements to optimize your roster.
        </p>
      </div>

      <div className="flex-1 grid grid-cols-2 gap-6">
        {/* Current Player */}
        <div className="bg-white border border-gray-300 rounded-lg p-6">
          <h3 className="text-lg font-semibold mb-4">Current Player</h3>
          <div className="space-y-4">
            <div>
              <h4 className="text-xl font-bold">{currentComparison.currentPlayer.name}</h4>
              <p className="text-gray-600">{currentComparison.currentPlayer.position}</p>
            </div>
            
            <div className="grid grid-cols-2 gap-4 text-sm">
              <div>
                <p className="text-gray-600">Salary</p>
                <p className="font-semibold">${currentComparison.currentPlayer.salary.toFixed(1)}M</p>
              </div>
              <div>
                <p className="text-gray-600">Health Risk</p>
                <span className={`px-2 py-1 rounded text-xs ${
                  currentComparison.currentPlayer.healthRisk === 'low' ? 'bg-green-100 text-green-800' :
                  currentComparison.currentPlayer.healthRisk === 'medium' ? 'bg-yellow-100 text-yellow-800' :
                  'bg-red-100 text-red-800'
                }`}>
                  {currentComparison.currentPlayer.healthRisk}
                </span>
              </div>
            </div>

            {currentComparison.currentPlayer.position !== 'P' && (
              <div className="grid grid-cols-3 gap-2 text-xs">
                <div>
                  <p className="text-gray-600">BA</p>
                  <p className="font-semibold">{currentComparison.currentPlayer.attributes.battingAverage.toFixed(3)}</p>
                </div>
                <div>
                  <p className="text-gray-600">HR</p>
                  <p className="font-semibold">{currentComparison.currentPlayer.attributes.homeRuns}</p>
                </div>
                <div>
                  <p className="text-gray-600">RBI</p>
                  <p className="font-semibold">{currentComparison.currentPlayer.attributes.rbis}</p>
                </div>
                <div>
                  <p className="text-gray-600">SB</p>
                  <p className="font-semibold">{currentComparison.currentPlayer.attributes.stolenBases}</p>
                </div>
                <div>
                  <p className="text-gray-600">OBP</p>
                  <p className="font-semibold">{currentComparison.currentPlayer.attributes.onBasePercentage.toFixed(3)}</p>
                </div>
                <div>
                  <p className="text-gray-600">SLG</p>
                  <p className="font-semibold">{currentComparison.currentPlayer.attributes.sluggingPercentage.toFixed(3)}</p>
                </div>
              </div>
            )}
          </div>
        </div>

        {/* Recommended Player */}
        <div className="bg-white border-2 border-blue-500 rounded-lg p-6">
          <h3 className="text-lg font-semibold mb-4">Recommended Replacement</h3>
          <div className="space-y-4">
            <div>
              <h4 className="text-xl font-bold">{currentComparison.recommendedPlayer.name}</h4>
              <p className="text-gray-600">{currentComparison.recommendedPlayer.position}</p>
            </div>
            
            <div className="grid grid-cols-2 gap-4 text-sm">
              <div>
                <p className="text-gray-600">Salary</p>
                <p className="font-semibold">${currentComparison.recommendedPlayer.salary.toFixed(1)}M</p>
              </div>
              <div>
                <p className="text-gray-600">Health Risk</p>
                <span className={`px-2 py-1 rounded text-xs ${
                  currentComparison.recommendedPlayer.healthRisk === 'low' ? 'bg-green-100 text-green-800' :
                  currentComparison.recommendedPlayer.healthRisk === 'medium' ? 'bg-yellow-100 text-yellow-800' :
                  'bg-red-100 text-red-800'
                }`}>
                  {currentComparison.recommendedPlayer.healthRisk}
                </span>
              </div>
            </div>

            {currentComparison.recommendedPlayer.position !== 'P' && (
              <div className="grid grid-cols-3 gap-2 text-xs">
                <div>
                  <p className="text-gray-600">BA</p>
                  <p className="font-semibold">{currentComparison.recommendedPlayer.attributes.battingAverage.toFixed(3)}</p>
                </div>
                <div>
                  <p className="text-gray-600">HR</p>
                  <p className="font-semibold">{currentComparison.recommendedPlayer.attributes.homeRuns}</p>
                </div>
                <div>
                  <p className="text-gray-600">RBI</p>
                  <p className="font-semibold">{currentComparison.recommendedPlayer.attributes.rbis}</p>
                </div>
                <div>
                  <p className="text-gray-600">SB</p>
                  <p className="font-semibold">{currentComparison.recommendedPlayer.attributes.stolenBases}</p>
                </div>
                <div>
                  <p className="text-gray-600">OBP</p>
                  <p className="font-semibold">{currentComparison.recommendedPlayer.attributes.onBasePercentage.toFixed(3)}</p>
                </div>
                <div>
                  <p className="text-gray-600">SLG</p>
                  <p className="font-semibold">{currentComparison.recommendedPlayer.attributes.sluggingPercentage.toFixed(3)}</p>
                </div>
              </div>
            )}
          </div>
        </div>
      </div>

      {/* Delta Metrics */}
      <div className="mt-6 bg-gradient-to-r from-blue-50 to-green-50 border border-gray-300 rounded-lg p-6">
        <h3 className="text-lg font-semibold mb-4">Impact Analysis</h3>
        <div className="grid grid-cols-3 gap-6">
          <div className="text-center">
            <div className="flex items-center justify-center gap-2 mb-2">
              {currentComparison.winDelta > 0 ? <ArrowIcon isUp={true} /> : <ArrowIcon isUp={false} />}
              <span className="text-2xl font-bold">
                {formatDelta(currentComparison.winDelta, currentComparison.winDelta > 0)}
              </span>
            </div>
            <p className="text-sm text-gray-600">Expected Wins</p>
          </div>
          <div className="text-center">
            <div className="flex items-center justify-center gap-2 mb-2">
              {currentComparison.salaryDelta < 0 ? <ArrowIcon isUp={true} /> : <ArrowIcon isUp={false} />}
              <span className="text-2xl font-bold">
                {formatDelta(currentComparison.salaryDelta, currentComparison.salaryDelta < 0)}
              </span>
            </div>
            <p className="text-sm text-gray-600">Salary (M)</p>
          </div>
          <div className="text-center">
            <div className="mb-2">
              <span className={`text-2xl font-bold ${
                currentComparison.winDelta > 0 && currentComparison.salaryDelta <= 0
                  ? 'text-green-600'
                  : currentComparison.winDelta > 0
                  ? 'text-yellow-600'
                  : 'text-red-600'
              }`}>
                {currentComparison.winDelta > 0 && currentComparison.salaryDelta <= 0
                  ? 'Recommended'
                  : currentComparison.winDelta > 0
                  ? 'Consider'
                  : 'Not Recommended'}
              </span>
            </div>
            <p className="text-sm text-gray-600">Recommendation</p>
          </div>
        </div>
      </div>

      {/* Radar Chart */}
      <div className="mt-6 bg-white border border-gray-300 rounded-lg p-6">
        <h3 className="text-lg font-semibold mb-4">Attribute Comparison</h3>
        <ResponsiveContainer width="100%" height={400}>
          <RadarChart data={radarData}>
            <PolarGrid />
            <PolarAngleAxis dataKey="attribute" />
            <PolarRadiusAxis angle={90} domain={[0, 1000]} />
            <Radar
              name="Current"
              dataKey="current"
              stroke="#ef4444"
              fill="#ef4444"
              fillOpacity={0.6}
            />
            <Radar
              name="Recommended"
              dataKey="recommended"
              stroke="#10b981"
              fill="#10b981"
              fillOpacity={0.6}
            />
            <Legend />
          </RadarChart>
        </ResponsiveContainer>
      </div>
    </div>
  );
}
