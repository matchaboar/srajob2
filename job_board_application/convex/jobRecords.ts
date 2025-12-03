import type { Doc } from "./_generated/dataModel";
import { formatLocationLabel, splitLocation } from "./location";

export type JobInsert = Omit<Doc<"jobs">, "_id" | "_creationTime">;

/**
 * Shape accepted by our builders before we derive location fields and timestamps.
 * All schema-required fields remain required here so type checking will surface
 * any schema changes that the generators are not updated to handle.
 */
export type JobSeed = Omit<JobInsert, "location" | "city" | "state" | "postedAt"> & {
  location: string;
  postedAt?: number;
  city?: JobInsert["city"];
  state?: JobInsert["state"];
};

export const buildJobInsert = (seed: JobSeed, now = Date.now()): JobInsert => {
  const { location, city: seedCity, state: seedState, postedAt, ...rest } = seed;
  const { city: parsedCity, state: parsedState } = splitLocation(location);
  const city = seedCity ?? parsedCity;
  const state = seedState ?? parsedState;
  const locationLabel = formatLocationLabel(city, state, location);
  const postedAtValue = postedAt ?? now;

  const base: JobInsert = {
    ...rest,
    location: locationLabel,
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
    description:
      "We're looking for a senior software engineer to join our growing team. You'll work on cutting-edge web applications using React, Node.js, and cloud technologies. Strong problem-solving skills and 5+ years of experience required.",
    location: "San Francisco, CA",
    remote: true,
    level: "senior",
    totalCompensation: 180000,
    url: "https://techcorp.com/careers/senior-engineer",
    postedAt: hoursAgo(now, 2),
    scrapedWith: "seed",
    workflowName: "seedData",
    scrapedCostMilliCents: 100, // 1/10¢
    compensationUnknown: false,
  },
  {
    title: "Frontend Developer",
    company: "StartupXYZ",
    description:
      "Join our fast-paced startup as a frontend developer! You'll be responsible for building beautiful, responsive user interfaces using React, TypeScript, and modern CSS frameworks. Perfect for someone who loves creating amazing user experiences.",
    location: "New York, NY",
    remote: false,
    level: "mid",
    totalCompensation: 120000,
    url: "https://startupxyz.com/jobs/frontend-dev",
    postedAt: hoursAgo(now, 4),
    scrapedWith: "seed",
    workflowName: "seedData",
    scrapedCostMilliCents: 10, // 1/100¢
    compensationUnknown: false,
  },
  {
    title: "Full Stack Developer",
    company: "Digital Solutions LLC",
    description:
      "We need a versatile full stack developer who can work across our entire technology stack. Experience with Python, Django, React, and PostgreSQL preferred. Great opportunity to work on diverse projects and grow your skills.",
    location: "Austin, TX",
    remote: true,
    level: "mid",
    totalCompensation: 135000,
    url: "https://digitalsolutions.com/careers/fullstack",
    postedAt: hoursAgo(now, 6),
    scrapedWith: "seed",
    workflowName: "seedData",
    scrapedCostMilliCents: 1, // 1/1000¢
    compensationUnknown: false,
  },
  {
    title: "Junior Web Developer",
    company: "WebWorks Agency",
    description:
      "Perfect entry-level position for a recent graduate or career changer! You'll learn modern web development practices while working on client projects. We provide mentorship and training in HTML, CSS, JavaScript, and React.",
    location: "Denver, CO",
    remote: false,
    level: "junior",
    totalCompensation: 75000,
    url: "https://webworks.com/jobs/junior-dev",
    postedAt: hoursAgo(now, 8),
    scrapedWith: "seed",
    workflowName: "seedData",
    scrapedCostMilliCents: 2500, // 2.50¢
    compensationUnknown: false,
  },
  {
    title: "Staff Software Engineer",
    company: "MegaTech Corporation",
    description:
      "Lead technical initiatives and mentor other engineers in our platform team. You'll architect scalable systems, drive technical decisions, and work on high-impact projects. 8+ years of experience and strong leadership skills required.",
    location: "Seattle, WA",
    remote: true,
    level: "staff",
    totalCompensation: 250000,
    url: "https://megatech.com/careers/staff-engineer",
    postedAt: hoursAgo(now, 10),
    scrapedWith: "seed",
    workflowName: "seedData",
    scrapedCostMilliCents: 1500, // 1.50¢
    compensationUnknown: false,
  },
  {
    title: "React Developer",
    company: "InnovateNow",
    description:
      "Specialized React developer needed for our product team. You'll build complex user interfaces, optimize performance, and collaborate with designers and backend engineers. Strong React, Redux, and TypeScript skills essential.",
    location: "Boston, MA",
    remote: true,
    level: "senior",
    totalCompensation: 165000,
    url: "https://innovatenow.com/jobs/react-dev",
    postedAt: hoursAgo(now, 12),
    scrapedWith: "seed",
    workflowName: "seedData",
    scrapedCostMilliCents: 500, // 1/2¢
    compensationUnknown: false,
  },
  {
    title: "Backend Engineer",
    company: "DataFlow Systems",
    description:
      "Join our backend team to build robust APIs and data processing systems. Experience with Node.js, Python, or Go required. You'll work on high-throughput systems handling millions of requests per day.",
    location: "Chicago, IL",
    remote: false,
    level: "mid",
    totalCompensation: 140000,
    url: "https://dataflow.com/careers/backend-engineer",
    postedAt: hoursAgo(now, 14),
    scrapedWith: "seed",
    workflowName: "seedData",
    scrapedCostMilliCents: 750, // 0.75¢
    compensationUnknown: false,
  },
  {
    title: "DevOps Engineer",
    company: "CloudFirst Technologies",
    description:
      "Help us scale our infrastructure and improve deployment processes. Experience with AWS, Docker, Kubernetes, and CI/CD pipelines required. You'll work on automation, monitoring, and security improvements.",
    location: "Portland, OR",
    remote: true,
    level: "senior",
    totalCompensation: 155000,
    url: "https://cloudfirst.com/jobs/devops",
    postedAt: hoursAgo(now, 16),
    scrapedWith: "seed",
    workflowName: "seedData",
    scrapedCostMilliCents: 2000, // 2.00¢
    compensationUnknown: false,
  },
  {
    title: "Mobile App Developer",
    company: "AppMakers Studio",
    description:
      "Develop cross-platform mobile applications using React Native. You'll work on consumer-facing apps with millions of users. Experience with mobile development, app store deployment, and performance optimization preferred.",
    location: "Los Angeles, CA",
    remote: false,
    level: "mid",
    totalCompensation: 130000,
    url: "https://appmakers.com/careers/mobile-dev",
    postedAt: hoursAgo(now, 18),
    scrapedWith: "seed",
    workflowName: "seedData",
    scrapedCostMilliCents: 999, // 0.999¢
    compensationUnknown: false,
  },
  {
    title: "Software Engineering Intern",
    company: "Future Tech Labs",
    description:
      "Summer internship program for computer science students. You'll work on real projects alongside experienced engineers, attend tech talks, and participate in code reviews. Great opportunity to gain industry experience.",
    location: "Palo Alto, CA",
    remote: false,
    level: "junior",
    totalCompensation: 45000,
    url: "https://futuretechlabs.com/internships/swe",
    postedAt: hoursAgo(now, 20),
    scrapedWith: "seed",
    workflowName: "seedData",
    scrapedCostMilliCents: 125, // 1/8¢
    compensationUnknown: false,
  },
];
