import { useState, useEffect } from "react";

interface LiveTimerProps {
    startTime: number;
    className?: string;
}

export function LiveTimer({ startTime, className }: LiveTimerProps) {
    const [currentTime, setCurrentTime] = useState(Date.now());

    useEffect(() => {
        const interval = setInterval(() => {
            setCurrentTime(Date.now());
        }, 1000);
        return () => clearInterval(interval);
    }, []);

    const formatElapsedTime = (timestamp: number) => {
        const elapsed = Math.floor((currentTime - timestamp) / 1000);
        const days = Math.floor(elapsed / 86400);
        const hours = Math.floor((elapsed % 86400) / 3600);
        const minutes = Math.floor((elapsed % 3600) / 60);
        const seconds = elapsed % 60;

        const timeString = `${hours.toString().padStart(2, '0')}:${minutes.toString().padStart(2, '0')}:${seconds.toString().padStart(2, '0')}`;

        if (days > 0) {
            return `${days} days, ${timeString}`;
        }
        return timeString;
    };

    return (
        <span className={className}>
            {formatElapsedTime(startTime)}
        </span>
    );
}
