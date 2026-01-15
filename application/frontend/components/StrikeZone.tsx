'use client';

import type { StrikeZoneData } from '@/lib/types';

interface StrikeZoneProps {
  strikeZone: StrikeZoneData;
}

export default function StrikeZone({ strikeZone }: StrikeZoneProps) {
  const { zones, target } = strikeZone;

  // Get heatmap color for a zone
  const getHeatColor = (heatValue: number) => {
    // Normalize heatValue from -1 to 1 to 0 to 1
    const normalized = (heatValue + 1) / 2;
    
    if (normalized < 0.5) {
      // Blue scale (weakness/whiffs) - darker blue for more negative values
      const blueIntensity = Math.floor((0.5 - normalized) * 2 * 255);
      return `rgb(${0}, ${50 + blueIntensity / 4}, ${150 + blueIntensity / 2})`;
    } else {
      // Red scale (strength/good contact) - darker red for more positive values
      const redIntensity = Math.floor((normalized - 0.5) * 2 * 255);
      return `rgb(${150 + redIntensity / 2}, ${50 + redIntensity / 4}, ${0})`;
    }
  };

  // Get opacity based on absolute heat value
  const getHeatOpacity = (heatValue: number) => {
    return Math.abs(heatValue) * 0.7 + 0.3;
  };

  // Create a 3x3 grid
  const grid: Array<Array<typeof zones[0] | null>> = [];
  for (let row = 0; row < 3; row++) {
    grid[row] = [];
    for (let col = 0; col < 3; col++) {
      const zone = zones.find((z) => z.row === row && z.col === col);
      grid[row][col] = zone || null;
    }
  }

  return (
    <div className="w-full rounded-lg border border-gray-200 bg-white p-6 shadow-sm">
      <h2 className="mb-4 text-center text-lg font-semibold text-gray-900">
        Strike Zone (Pitcher&apos;s Perspective)
      </h2>
      <div className="mx-auto aspect-square max-w-md">
        <div className="grid h-full grid-cols-3 grid-rows-3 gap-1 border-2 border-gray-800">
          {grid.map((row, rowIndex) =>
            row.map((zone, colIndex) => {
              if (!zone) return null;

              const isTarget =
                target.row === rowIndex && target.col === colIndex;
              const heatColor = getHeatColor(zone.heatValue);
              const heatOpacity = getHeatOpacity(zone.heatValue);

              return (
                <div
                  key={`${rowIndex}-${colIndex}`}
                  className="relative flex items-center justify-center border border-gray-400"
                  style={{
                    backgroundColor: heatColor,
                    opacity: heatOpacity,
                  }}
                >
                  {/* Target indicator */}
                  {isTarget && (
                    <div className="absolute z-10 h-16 w-16 animate-pulse rounded-full border-4 border-yellow-400 bg-yellow-300 shadow-lg" />
                  )}
                  
                  {/* Zone indicator for debugging */}
                  <div className="absolute bottom-1 right-1 text-xs font-bold text-white opacity-50">
                    {zone.heatValue.toFixed(2)}
                  </div>
                </div>
              );
            })
          )}
        </div>
        
        {/* Legend */}
        <div className="mt-4 flex items-center justify-center gap-6 text-xs">
          <div className="flex items-center gap-2">
            <div className="h-4 w-4 rounded bg-blue-500" />
            <span className="text-gray-600">Batter Weakness (Whiff)</span>
          </div>
          <div className="flex items-center gap-2">
            <div className="h-4 w-4 rounded bg-red-500" />
            <span className="text-gray-600">Batter Strength</span>
          </div>
          <div className="flex items-center gap-2">
            <div className="h-4 w-4 rounded-full border-2 border-yellow-400 bg-yellow-300" />
            <span className="text-gray-600">Target</span>
          </div>
        </div>
      </div>
    </div>
  );
}
