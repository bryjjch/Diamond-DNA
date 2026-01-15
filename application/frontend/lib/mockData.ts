import type { PitchCallData } from './types';

// Generate random value between min and max
const randomBetween = (min: number, max: number) => {
  return Math.random() * (max - min) + min;
};

// Common pitch types
const pitchTypes = [
  'Fastball',
  'Slider',
  'Curveball',
  'Changeup',
  'Cutter',
  'Splitter',
  'Sinker',
];

// Zone descriptions for location
const zoneLabels = [
  'High Inside',
  'High Middle',
  'High Outside',
  'Middle Inside',
  'Middle',
  'Middle Outside',
  'Low Inside',
  'Low Middle',
  'Low Outside',
];

// Sample batter names
const batterNames = [
  'Mike Trout',
  'Aaron Judge',
  'Mookie Betts',
  'Ronald Acuña Jr.',
  'Juan Soto',
  'Vladimir Guerrero Jr.',
  'Fernando Tatis Jr.',
  'Shohei Ohtani',
];

export const generateMockPitchCallData = (): PitchCallData => {
  // Generate random pitch call
  const pitchType = pitchTypes[Math.floor(Math.random() * pitchTypes.length)];
  const zoneIndex = Math.floor(Math.random() * zoneLabels.length);
  const row = Math.floor(zoneIndex / 3);
  const col = zoneIndex % 3;
  const confidence = Math.floor(randomBetween(60, 95));

  // Generate strike zone heatmap data
  const strikeZones = [];
  for (let r = 0; r < 3; r++) {
    for (let c = 0; c < 3; c++) {
      // Generate heat values (-1 to 1) with some randomness
      const heatValue = randomBetween(-0.8, 0.8);
      strikeZones.push({ row: r, col: c, heatValue });
    }
  }

  // Generate random game state
  const balls = Math.floor(Math.random() * 4);
  const strikes = Math.floor(Math.random() * 3);
  const batterName = batterNames[Math.floor(Math.random() * batterNames.length)];
  const pitcherEnergy = Math.floor(randomBetween(30, 100));

  return {
    call: {
      pitchType,
      location: { zone: zoneLabels[zoneIndex] },
      confidence,
    },
    strikeZone: {
      zones: strikeZones,
      target: { row, col },
    },
    gameState: {
      batterName,
      count: { balls, strikes },
      pitcherEnergy,
    },
  };
};

// Initial mock data
export const getMockPitchCallData = (): PitchCallData => {
  return generateMockPitchCallData();
};
