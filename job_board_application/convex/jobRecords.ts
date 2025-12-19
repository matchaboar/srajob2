import type { Doc } from "./_generated/dataModel";
import { deriveLocationFields, formatLocationLabel } from "./location";

export type JobInsert = Omit<Doc<"jobs">, "_id" | "_creationTime">;
export type JobDetailInsert = Omit<Doc<"job_details">, "_id" | "_creationTime" | "jobId">;

/**
 * Shape accepted by our builders before we derive location fields and timestamps.
 * All schema-required fields remain required here so type checking will surface
 * any schema changes that the generators are not updated to handle.
 */
export type JobSeed = Omit<JobInsert, "location" | "city" | "state" | "postedAt"> & {
  location: string;
  locations?: string[];
  countries?: string[];
  country?: string;
  locationStates?: string[];
  locationSearch?: string;
  postedAt?: number;
  city?: JobInsert["city"];
  state?: JobInsert["state"];
  details?: JobDetailInsert;
};

export const buildJobInsert = (seed: JobSeed, now = Date.now()): JobInsert => {
  const {
    location,
    locations: seedLocations,
    city: seedCity,
    state: seedState,
    postedAt,
    details: _details,
    ...rest
  } = seed;
  const locationInfo = deriveLocationFields({ locations: seedLocations ?? [location], location });
  const city = seedCity ?? locationInfo.city;
  const state = seedState ?? locationInfo.state;
  const locationLabel = formatLocationLabel(city, state, locationInfo.primaryLocation);
  const postedAtValue = postedAt ?? now;

  const base: JobInsert = {
    ...rest,
    location: locationLabel,
    locations: locationInfo.locations,
    countries: locationInfo.countries,
    country: locationInfo.country,
    locationStates: locationInfo.locationStates,
    locationSearch: locationInfo.locationSearch,
    city,
    state,
    postedAt: postedAtValue,
  };

  // Default scrapedAt to the publish time when it is not provided.
  if (base.scrapedAt === undefined) {
    base.scrapedAt = postedAtValue;
  }

  return base;
};

const hoursAgo = (now: number, hours: number) => now - hours * 60 * 60 * 1000;

export const makeFakeJobSeeds = (now = Date.now()): JobSeed[] => [
  {
    title: "Senior Software Engineer",
    company: "TechCorp Inc.",
    location: "San Francisco, CA",
    remote: true,
    level: "senior",
    totalCompensation: 180000,
    url: "https://techcorp.com/careers/senior-engineer",
    postedAt: hoursAgo(now, 2),
    compensationUnknown: false,
    details: {
      description:
        "We're looking for a senior software engineer to join our growing team. You'll work on cutting-edge web applications using React, Node.js, and cloud technologies. Strong problem-solving skills and 5+ years of experience required.",
      scrapedWith: "seed",
      workflowName: "seedData",
      scrapedCostMilliCents: 100, // 1/10¢
    },
  },
  {
    title: "Frontend Developer",
    company: "StartupXYZ",
    location: "New York, NY",
    remote: false,
    level: "mid",
    totalCompensation: 120000,
    url: "https://startupxyz.com/jobs/frontend-dev",
    postedAt: hoursAgo(now, 4),
    compensationUnknown: false,
    details: {
      description:
        "Join our fast-paced startup as a frontend developer! You'll be responsible for building beautiful, responsive user interfaces using React, TypeScript, and modern CSS frameworks. Perfect for someone who loves creating amazing user experiences.",
      scrapedWith: "seed",
      workflowName: "seedData",
      scrapedCostMilliCents: 10, // 1/100¢
    },
  },
  {
    title: "Full Stack Developer",
    company: "Digital Solutions LLC",
    location: "Austin, TX",
    remote: true,
    level: "mid",
    totalCompensation: 135000,
    url: "https://digitalsolutions.com/careers/fullstack",
    postedAt: hoursAgo(now, 6),
    compensationUnknown: false,
    details: {
      description:
        "We need a versatile full stack developer who can work across our entire technology stack. Experience with Python, Django, React, and PostgreSQL preferred. Great opportunity to work on diverse projects and grow your skills.",
      scrapedWith: "seed",
      workflowName: "seedData",
      scrapedCostMilliCents: 1, // 1/1000¢
    },
  },
  {
    title: "Junior Web Developer",
    company: "WebWorks Agency",
    location: "Denver, CO",
    remote: false,
    level: "junior",
    totalCompensation: 75000,
    url: "https://webworks.com/jobs/junior-dev",
    postedAt: hoursAgo(now, 8),
    compensationUnknown: false,
    details: {
      description:
        "Perfect entry-level position for a recent graduate or career changer! You'll learn modern web development practices while working on client projects. We provide mentorship and training in HTML, CSS, JavaScript, and React.",
      scrapedWith: "seed",
      workflowName: "seedData",
      scrapedCostMilliCents: 2500, // 2.50¢
    },
  },
  {
    title: "Staff Software Engineer",
    company: "MegaTech Corporation",
    location: "Seattle, WA",
    remote: true,
    level: "staff",
    totalCompensation: 250000,
    url: "https://megatech.com/careers/staff-engineer",
    postedAt: hoursAgo(now, 10),
    compensationUnknown: false,
    details: {
      description:
        "Lead technical initiatives and mentor other engineers in our platform team. You'll architect scalable systems, drive technical decisions, and work on high-impact projects. 8+ years of experience and strong leadership skills required.",
      scrapedWith: "seed",
      workflowName: "seedData",
      scrapedCostMilliCents: 1500, // 1.50¢
    },
  },
  {
    title: "React Developer",
    company: "InnovateNow",
    location: "Boston, MA",
    remote: true,
    level: "senior",
    totalCompensation: 165000,
    url: "https://innovatenow.com/jobs/react-dev",
    postedAt: hoursAgo(now, 12),
    compensationUnknown: false,
    details: {
      description:
        "Specialized React developer needed for our product team. You'll build complex user interfaces, optimize performance, and collaborate with designers and backend engineers. Strong React, Redux, and TypeScript skills essential.",
      scrapedWith: "seed",
      workflowName: "seedData",
      scrapedCostMilliCents: 500, // 1/2¢
    },
  },
  {
    title: "Backend Engineer",
    company: "DataFlow Systems",
    location: "Chicago, IL",
    remote: false,
    level: "mid",
    totalCompensation: 140000,
    url: "https://dataflow.com/careers/backend-engineer",
    postedAt: hoursAgo(now, 14),
    compensationUnknown: false,
    details: {
      description:
        "Join our backend team to build robust APIs and data processing systems. Experience with Node.js, Python, or Go required. You'll work on high-throughput systems handling millions of requests per day.",
      scrapedWith: "seed",
      workflowName: "seedData",
      scrapedCostMilliCents: 750, // 0.75¢
    },
  },
  {
    title: "DevOps Engineer",
    company: "CloudFirst Technologies",
    location: "Portland, OR",
    remote: true,
    level: "senior",
    totalCompensation: 155000,
    url: "https://cloudfirst.com/jobs/devops",
    postedAt: hoursAgo(now, 16),
    compensationUnknown: false,
    details: {
      description:
        "Help us scale our infrastructure and improve deployment processes. Experience with AWS, Docker, Kubernetes, and CI/CD pipelines required. You'll work on automation, monitoring, and security improvements.",
      scrapedWith: "seed",
      workflowName: "seedData",
      scrapedCostMilliCents: 2000, // 2.00¢
    },
  },
  {
    title: "Mobile App Developer",
    company: "AppMakers Studio",
    location: "Los Angeles, CA",
    remote: false,
    level: "mid",
    totalCompensation: 130000,
    url: "https://appmakers.com/careers/mobile-dev",
    postedAt: hoursAgo(now, 18),
    compensationUnknown: false,
    details: {
      description:
        "Develop cross-platform mobile applications using React Native. You'll work on consumer-facing apps with millions of users. Experience with mobile development, app store deployment, and performance optimization preferred.",
      scrapedWith: "seed",
      workflowName: "seedData",
      scrapedCostMilliCents: 999, // 0.999¢
    },
  },
  {
    title: "Software Engineering Intern",
    company: "Future Tech Labs",
    location: "Palo Alto, CA",
    remote: false,
    level: "junior",
    totalCompensation: 45000,
    url: "https://futuretechlabs.com/internships/swe",
    postedAt: hoursAgo(now, 20),
    compensationUnknown: false,
    details: {
      description:
        "Summer internship program for computer science students. You'll work on real projects alongside experienced engineers, attend tech talks, and participate in code reviews. Great opportunity to gain industry experience.",
      scrapedWith: "seed",
      workflowName: "seedData",
      scrapedCostMilliCents: 125, // 1/8¢
    },
  },
];
