'use client';

import type { GameState } from '@/lib/types';

interface GameStateHeaderProps {
  gameState: GameState;
}

export default function GameStateHeader({ gameState }: GameStateHeaderProps) {
  const { batterName, count, pitcherEnergy } = gameState;
  const countDisplay = `${count.balls}-${count.strikes}`;

  // Determine energy bar color based on pitcher energy
  const getEnergyColor = (energy: number) => {
    if (energy >= 70) return 'bg-green-500';
    if (energy >= 40) return 'bg-yellow-500';
    return 'bg-red-500';
  };

  return (
    <div className="w-full border-b border-gray-200 bg-gradient-to-r from-gray-50 to-gray-100 px-4 py-4 shadow-sm">
      <div className="mx-auto flex flex-col gap-4 sm:flex-row sm:items-center sm:justify-between">
        <div className="flex flex-col gap-1 sm:flex-row sm:items-center sm:gap-4">
          <div>
            <p className="text-xs font-medium text-gray-500 uppercase tracking-wide">
              Batter
            </p>
            <p className="text-xl font-bold text-gray-900">{batterName}</p>
          </div>

          <div className="border-l border-gray-300 pl-4 sm:pl-4">
            <p className="text-xs font-medium text-gray-500 uppercase tracking-wide">
              Count
            </p>
            <p className="text-2xl font-bold text-gray-900">{countDisplay}</p>
          </div>
        </div>

        <div className="flex-1 sm:max-w-xs">
          <div className="flex items-center justify-between text-sm">
            <span className="font-medium text-gray-700">Pitcher Energy</span>
            <span className="font-semibold text-gray-900">{pitcherEnergy}%</span>
          </div>
          <div className="mt-2 h-3 w-full overflow-hidden rounded-full bg-gray-300">
            <div
              className={`h-full ${getEnergyColor(pitcherEnergy)} transition-all duration-500`}
              style={{ width: `${pitcherEnergy}%` }}
            />
          </div>
        </div>
      </div>
    </div>
  );
}
