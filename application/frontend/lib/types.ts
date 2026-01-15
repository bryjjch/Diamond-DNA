export interface PitchCall {
  pitchType: string;
  location: { x: number; y: number } | { zone: string };
  confidence: number; // 0-100
}

export interface StrikeZoneData {
  zones: Array<{
    row: number; // 0-2 (top to bottom)
    col: number; // 0-2 (left to right)
    heatValue: number; // -1 to 1 (negative = weakness/blue, positive = strength/red)
  }>;
  target: { row: number; col: number };
}

export interface GameState {
  batterName: string;
  count: { balls: number; strikes: number };
  pitcherEnergy: number; // 0-100
}

export type FeedbackType =
  | 'ball'
  | 'called_strike'
  | 'swinging_strike'
  | 'foul'
  | 'in_play_out'
  | 'in_play_hit';

export interface PitchCallData {
  call: PitchCall;
  strikeZone: StrikeZoneData;
  gameState: GameState;
}
