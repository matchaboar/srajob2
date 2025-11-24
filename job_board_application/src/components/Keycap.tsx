import React from "react";

interface KeycapProps {
    label: string;
    className?: string;
}

export const Keycap: React.FC<KeycapProps> = ({ label, className }) => {
    return (
        <k-cap className={`custom-theme ${className || ''}`}>
            <k-legend className="center medium">{label}</k-legend>
        </k-cap>
    );
};
