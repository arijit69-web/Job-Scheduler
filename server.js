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

const fetchJobs = async (params) => {
    try {
        const json = await new Promise((resolve, reject) => {
            getJson(params, (result, error) => {
                if (error) return reject(error);
                if (!result || !result.jobs_results) {
                    return reject(new Error('Invalid API response: jobs_results missing.'));
                }
                resolve(result);
            });
        });
        return json;
    } catch (error) {
        console.error('Error fetching jobs from API:', error.message);
        throw error;
    }
};

const processAndStoreJobs = async () => {
    try {
        console.log('Starting job fetch and store process...');

        const params = {
            api_key: process.env.SERPAPI_KEY,
            engine: 'google_jobs',
            google_domain: 'google.co.in',
            location: 'India',
            q: 'Software Engineer Freshers in the last week',
        };

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

        const insertedJobs = [];
        for (const job of processedJobs) {
            try {
                const insertedJob = await Job.create(job);
                insertedJobs.push(insertedJob);
            } catch (err) {
                if (err.code === 11000) {
                    console.warn(`Duplicate job_id skipped: ${job.job_id}`);
                } else {
                    console.error(`Error inserting job: ${job.job_id}`, err.message);
                }
            }
        }

        console.log(`${insertedJobs.length} new jobs stored in MongoDB.`);
    } catch (err) {
        console.error('Error during job processing:', err.message);
    }
};

cron.schedule(
    '0 0 */2 * *',
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
