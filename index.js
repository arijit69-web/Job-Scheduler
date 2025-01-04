require('dotenv').config();
const express = require('express');
const mongoose = require('mongoose');
const { getJson } = require('serpapi');
const cron = require('node-cron');

const jobSchema = new mongoose.Schema({
    job_id: { type: String, unique: true, required: true, index: true },
    jobTitle: { type: String, required: true },
    companyName: { type: String, required: true },
    jobLocation: { type: String, required: true },
    companyLogo: String,
    postingDate: String,
    employmentType: String,
    jobUrl: String,
}, { timestamps: true });

const Job = mongoose.model('Job', jobSchema);
Job.createIndexes();

const app = express();
const PORT = process.env.PORT || 3000;

const connectToDatabase = async () => {
    try {
        await mongoose.connect(process.env.MONGODB_URI, {
            useNewUrlParser: true,
            useUnifiedTopology: true,
        });
        console.log('Connected to MongoDB');
    } catch (err) {
        console.error('MongoDB connection error:', err.message);
        process.exit(1);
    }
};
connectToDatabase();

app.use(express.json());

const TOP_20_MNCs = [
    'Google', 'Amazon', 'Microsoft', 'Apple', 'Facebook',
    'Intel', 'IBM', 'Cisco', 'Oracle', 'SAP',
    'Samsung', 'Sony', 'Dell', 'Adobe', 'HP',
    'NVIDIA', 'Salesforce', 'Accenture', 'Infosys', 'TCS'
];

const fetchJobs = async (params) => {
    try {
        const json = await new Promise((resolve, reject) => {
            getJson(params, (result, error) => {
                if (result && result.error) {
                    console.error("Error in fetching jobs: ", result.error);
                    return reject(new Error(`Error in fetching jobs for: ${params.q}, Error: ${result.error}`));
                }
                if (error) {
                    console.error("Request Error: ", error);
                    return reject(new Error(`Request failed for: ${params.q}, Error: ${error.message}`));
                }
                if (!result || !result.jobs_results) {
                    console.error("Invalid API response: jobs_results missing.", result);
                    return reject(new Error('Invalid API response: jobs_results missing.'));
                }
                resolve(result);
            });
        });
        return json;
    } catch (error) {
        console.error('Error fetching jobs from API:', error.message, "Params:: ", params);
        throw error;
    }
};


const processAndStoreJobs = async () => {

    console.log('Starting job fetch and store process...');
    let insertedJobsCount = 0;

    for (const company of TOP_20_MNCs) {
        console.log(`Fetching jobs for: ${company}`);
        const params = {
            api_key: process.env.SERPAPI_KEY,
            engine: 'google_jobs',
            google_domain: 'google.co.in',
            location: 'India',
            q: `Software Engineer ${company}`,
            no_cache: "true"

        };

        try {
            const json = await fetchJobs(params);

            const jobsArray = (json.jobs_results || []);
            const processedJobs = jobsArray.map(job => ({
                job_id: job.job_id,
                jobTitle: job.title,
                companyName: job.company_name,
                jobLocation: job.location,
                companyLogo: job.thumbnail || null,
                postingDate: job.detected_extensions?.posted_at || null,
                employmentType: job.detected_extensions?.scheduled_type || null,
                jobUrl: job.apply_options?.[0].link || null,
            }));

            for (const job of processedJobs) {
                try {
                    await Job.create(job);
                    insertedJobsCount++;
                } catch (err) {
                    if (err.code === 11000) {
                        console.warn(`Duplicate job_id skipped: ${job.job_id}`);
                    } else {
                        console.error(`Error inserting job: ${job.job_id}`, err.message);
                    }
                }
            }
        }
        catch (err) {
            console.error("Error in fetching jobs for: ", company, "Error: ", err.message);
            continue;
        }

    }
    console.log(`${insertedJobsCount} new jobs stored in MongoDB.`);

};
process.on('unhandledRejection', (reason, promise) => {
    console.error('Promise Rejection :: Reason: ', reason);
});

cron.schedule(
    '0 0 * * 0',//Sunday, at 00:00
    async () => {
        console.log('Cron job triggered: Fetching jobs...');
        await processAndStoreJobs();
    },
    {
        scheduled: true,
        timezone: 'Asia/Kolkata',
    }
);

app.get('/', async (req, res) => {
    res.status(200).json({ message: 'Job fetcher service is running.' });
});

app.listen(PORT, () => {
    console.log(`Server is running on port ${PORT}`);
});
